DO $$
BEGIN
    FOR counter IN 1..10 LOOP
        INSERT INTO flight_airline (flight_id, airline)
            VALUES (
                (SELECT flight_id FROM (
                    SELECT ROW_NUMBER() OVER() row_number, flight_id FROM flight_route
                ) tab WHERE row_number = counter),
                (SELECT airline_id FROM (
                    SELECT ROW_NUMBER() OVER() row_number, airline_id FROM airlines
                ) tab WHERE row_number = counter)
            );
    END LOOP;
END;
$$