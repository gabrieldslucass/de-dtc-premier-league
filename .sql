/*
Please, write your SQL query below.
*/

WITH hotel_booking_count AS (
    SELECT 
        h.city_id, 
        h.id AS hotel_id, 
        MAX(b.booking_date) AS last_booking_date,
        COUNT(b.id) AS booking_count
    FROM hotel h
    LEFT JOIN booking b ON h.id = b.hotel_id
    GROUP BY h.city_id, h.id
)
select 
    c.name AS city,
    hbc.last_booking_date,
    hbc.hotel_id,
    h.photos->>0 AS hotel_photo

FROM hotel_booking_count hbc
JOIN hotel h ON  h.id = hbc.hotel_id
JOIN city c ON c.id = hbc.city_id

WHERE hbc.booking_count = (
    SELECT MAX(booking_count) 
    FROM hotel_booking_count 
    WHERE city_id = hbc.city_id
)
ORDER BY c.name, h.id;


-- test
WITH hotel_booking_count AS (
    SELECT 
        h.city_id, 
        h.id AS hotel_id, 
        MAX(b.booking_date) AS last_booking_date,
        COUNT(b.id) AS booking_count
    FROM hotel h
    LEFT JOIN booking b ON h.id = b.hotel_id
    GROUP BY h.city_id, h.id
),
ranked_hotels AS (
    SELECT *,
        RANK() OVER (
            PARTITION BY city_id 
            ORDER BY booking_count DESC, last_booking_date DESC, hotel_id ASC
        ) AS rnk
    FROM hotel_booking_count
)
SELECT 
    c.name AS city,
    rh.last_booking_date,
    rh.hotel_id,
    h.photos->>0 AS hotel_photo
FROM ranked_hotels rh
JOIN hotel h ON h.id = rh.hotel_id
JOIN city c ON c.id = rh.city_id
WHERE rh.rnk = 1
ORDER BY c.name;


/*
Please, write your SQL query below.
*/


WITH last_booking AS (
	-- Select the last booking date grouping by city
    SELECT 
        h.city_id,
        MAX(b.booking_date) AS last_booking_date
    FROM booking b
    JOIN hotel h ON b.hotel_id = h.id
    GROUP BY h.city_id
),
hotel_booking_count AS (
	-- Rank hotels by booking count and partitioned by city
SELECT 
        h.city_id,
        h.id AS hotel_id,
        h.photos[0] AS hotel_photo,
        ROW_NUMBER() OVER (PARTITION BY h.city_id ORDER BY COUNT(b.id) DESC) AS rn
    FROM hotel h
    LEFT JOIN booking b ON h.id = b.hotel_id
    GROUP BY h.city_id, h.id, h.photos
)
SELECT 
    c."name" AS city,
    lb.last_booking_date,
    hbc.hotel_id,
    hbc.hotel_photo,
    hbc.rn
FROM hotel_booking_count hbc
JOIN city c ON hbc.city_id = c.id 
JOIN last_booking lb ON c.id = lb.city_id
WHERE hbc.rn = 1
ORDER BY city, hbc.hotel_id;
