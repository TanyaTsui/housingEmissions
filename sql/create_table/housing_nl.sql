CREATE TABLE IF NOT EXISTS housing_nl (
    function TEXT,
    sqm BIGINT,
    id_pand VARCHAR,
    geometry VARCHAR,
    build_year VARCHAR,
    status VARCHAR,
    document_date VARCHAR,
    document_number VARCHAR,
    registration_start VARCHAR,
    registration_end VARCHAR,
    geom GEOMETRY,             -- Assuming GEOMETRY is the intended type
    geom_28992 GEOMETRY,       -- Assuming GEOMETRY is the intended type
    neighborhood_code VARCHAR, -- Added column
    neighborhood VARCHAR,      -- Added column
    municipality VARCHAR,
    province VARCHAR
);