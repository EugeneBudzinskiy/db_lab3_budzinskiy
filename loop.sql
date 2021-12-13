DO $$
BEGIN
    FOR counter IN 1..10 LOOP
        INSERT INTO airlines (airline_id, airline_name)
            VALUES (counter, 'test');
    END LOOP;
END;
$$