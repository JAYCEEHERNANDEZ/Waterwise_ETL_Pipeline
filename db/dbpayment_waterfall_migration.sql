DROP FUNCTION IF EXISTS record_payment_waterfall(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION record_payment_waterfall(
    p_consumer_id INTEGER,
    p_applied NUMERIC,
    p_amount_tendered NUMERIC,
    p_payment_date DATE,
    p_payment_method TEXT,
    p_reference_number TEXT,
    p_idempotency_key TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_bill RECORD;
    v_left NUMERIC(10,2) := ROUND(p_applied, 2);
    v_slice NUMERIC(10,2);
    v_next_balance NUMERIC(10,2);
    v_next_status VARCHAR(30);
    v_payment payments%ROWTYPE;
    v_updated billing%ROWTYPE;
    v_payments JSONB := '[]'::JSONB;
    v_billings JSONB := '[]'::JSONB;
    v_existing payments%ROWTYPE;
    v_replay_total NUMERIC(10,2);
    v_replay_consumer INTEGER;
    v_replay_method TEXT;
BEGIN
    IF p_applied IS NULL OR ROUND(p_applied, 2) <= 0 THEN
        RAISE EXCEPTION 'Payment amount must be greater than zero.';
    END IF;
    IF p_amount_tendered IS NULL OR ROUND(p_amount_tendered, 2) <= 0 THEN
        RAISE EXCEPTION 'Amount received must be greater than zero.';
    END IF;
    IF p_payment_method NOT IN ('Cash', 'GCash') THEN
        RAISE EXCEPTION 'Unsupported payment method.';
    END IF;
    IF p_payment_date IS NULL THEN
        RAISE EXCEPTION 'Payment date is required.';
    END IF;
    IF p_payment_method <> 'Cash' AND NULLIF(TRIM(COALESCE(p_reference_number, '')), '') IS NULL THEN
        RAISE EXCEPTION 'An electronic payment reference number is required.';
    END IF;
    IF p_payment_method = 'Cash' AND ROUND(p_amount_tendered, 2) < ROUND(p_applied, 2) THEN
        RAISE EXCEPTION 'Cash received cannot be lower than the amount applied.';
    END IF;
    IF p_payment_method <> 'Cash' AND ROUND(p_amount_tendered, 2) <> ROUND(p_applied, 2) THEN
        RAISE EXCEPTION 'Electronic payment must equal the amount applied.';
    END IF;
    IF p_idempotency_key IS NULL OR TRIM(p_idempotency_key) = '' OR CHAR_LENGTH(TRIM(p_idempotency_key)) > 200 THEN
        RAISE EXCEPTION 'A valid payment idempotency key is required.';
    END IF;

    SELECT * INTO v_existing FROM payments WHERE starts_with(idempotency_key, TRIM(p_idempotency_key) || ':') LIMIT 1;
    IF FOUND THEN
        SELECT MIN(b.user_id), SUM(p.total_paid), MIN(p.payment_method) INTO v_replay_consumer, v_replay_total, v_replay_method
        FROM payments p JOIN billing b ON b.id = p.billing_id
        WHERE starts_with(p.idempotency_key, TRIM(p_idempotency_key) || ':');
        IF v_replay_consumer IS DISTINCT FROM p_consumer_id OR ROUND(COALESCE(v_replay_total, 0), 2) <> ROUND(p_applied, 2) THEN
            RAISE EXCEPTION 'This payment request key was already used for different payment details.';
        END IF;
        SELECT jsonb_agg(to_jsonb(p) - 'idempotency_key') INTO v_payments FROM payments p WHERE starts_with(p.idempotency_key, TRIM(p_idempotency_key) || ':');
        SELECT jsonb_agg(to_jsonb(b)) INTO v_billings FROM billing b WHERE b.id IN (SELECT p.billing_id FROM payments p WHERE starts_with(p.idempotency_key, TRIM(p_idempotency_key) || ':'));
        v_replay_total := ROUND(COALESCE(v_replay_total, 0), 2);
        RETURN jsonb_build_object(
            'payments', COALESCE(v_payments, '[]'::JSONB),
            'billings', COALESCE(v_billings, '[]'::JSONB),
            'total_applied', v_replay_total,
            'change_given', CASE WHEN v_replay_method = 'Cash' THEN ROUND(p_amount_tendered - v_replay_total, 2) ELSE 0.00 END,
            'replayed', true
        );
    END IF;

    FOR v_bill IN SELECT * FROM billing WHERE user_id = p_consumer_id AND remaining_balance > 0 ORDER BY billing_date ASC, id ASC FOR UPDATE LOOP
        EXIT WHEN v_left <= 0;
        v_slice := LEAST(ROUND(v_bill.remaining_balance, 2), v_left);
        v_next_balance := ROUND(v_bill.remaining_balance - v_slice, 2);
        v_next_status := CASE WHEN v_next_balance = 0 THEN 'Paid' ELSE 'Partially Paid' END;

        INSERT INTO payments (billing_id, total_paid, amount_tendered, change_given, remaining_balance, payment_date, payment_method, reference_number, idempotency_key)
        VALUES (v_bill.id, v_slice, v_slice, 0.00, v_next_balance, p_payment_date, p_payment_method, NULLIF(TRIM(COALESCE(p_reference_number, '')), ''), TRIM(p_idempotency_key) || ':' || v_bill.id)
        RETURNING * INTO v_payment;

        UPDATE billing SET remaining_balance = v_next_balance, status = v_next_status, updated_at = NOW() WHERE id = v_bill.id RETURNING * INTO v_updated;

        INSERT INTO notifications (consumer_id, announcement_type, notification_type, priority, title, announcement_date, message, billing_id, payment_id, action_path, event_key)
        VALUES (v_bill.user_id, 'Payment Alert', 'payment_received', CASE WHEN v_next_balance = 0 THEN 'normal' ELSE 'high' END,
            CASE WHEN v_next_balance = 0 THEN 'Payment received' ELSE 'Partial payment received' END,
            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE,
            CASE WHEN v_next_balance = 0 THEN format('We received your payment of PHP %s for bill #%s. This bill is now fully paid.', v_slice, v_bill.id)
                 ELSE format('We received your payment of PHP %s for bill #%s. Remaining balance: PHP %s.', v_slice, v_bill.id, v_next_balance) END,
            v_bill.id, v_payment.id, '/consumer/billing-ledger', format('payment-received:%s', v_payment.id))
        ON CONFLICT (event_key) DO NOTHING;

        v_payments := v_payments || jsonb_build_array((to_jsonb(v_payment) - 'idempotency_key'));
        v_billings := v_billings || jsonb_build_array(to_jsonb(v_updated));
        v_left := ROUND(v_left - v_slice, 2);
    END LOOP;

    IF ROUND(v_left, 2) > 0 AND p_payment_method <> 'Cash' THEN
        RAISE EXCEPTION 'Electronic payments cannot exceed the total outstanding balance.';
    END IF;

    IF v_payments = '[]'::JSONB THEN
        RAISE EXCEPTION 'No outstanding bills for this resident.';
    END IF;

    RETURN jsonb_build_object(
        'payments', v_payments,
        'billings', v_billings,
        'total_applied', (SELECT COALESCE(SUM((x->>'total_paid')::NUMERIC), 0) FROM jsonb_array_elements(v_payments) x),
        'change_given', CASE WHEN p_payment_method = 'Cash' THEN ROUND(p_amount_tendered - p_applied, 2) ELSE 0.00 END,
        'replayed', false
    );
END;
$$;

REVOKE ALL ON FUNCTION record_payment_waterfall(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION record_payment_waterfall(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) FROM anon;
REVOKE ALL ON FUNCTION record_payment_waterfall(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) FROM authenticated;
GRANT EXECUTE ON FUNCTION record_payment_waterfall(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) TO service_role;