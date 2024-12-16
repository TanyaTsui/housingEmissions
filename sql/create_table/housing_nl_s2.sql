CREATE TABLE IF NOT EXISTS housing_nl_s2 (
    municipality CHARACTER VARYING,
    wk_code CHARACTER VARYING,
    bu_code CHARACTER VARYING,
    id_pand CHARACTER VARYING,
    pand_geom GEOMETRY,
    status TEXT,
    function TEXT,
    sqm BIGINT,
    build_year CHARACTER VARYING,
    document_date CHARACTER VARYING,
    document_number CHARACTER VARYING,
    registration_start CHARACTER VARYING,
    registration_end CHARACTER VARYING
);
