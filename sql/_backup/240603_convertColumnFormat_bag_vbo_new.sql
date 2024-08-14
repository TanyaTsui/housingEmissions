ALTER TABLE bag_vbo_new
	ALTER COLUMN sqm TYPE integer USING sqm::integer,
	ALTER COLUMN document_date TYPE date USING document_date::date,
	ALTER COLUMN registration_start TYPE date USING registration_start::date,
	ALTER COLUMN registration_end TYPE date USING registration_end::date,
	ALTER COLUMN p_document_date TYPE date USING p_document_date::date,
	ALTER COLUMN p_registration_start TYPE date USING p_registration_start::date,
	ALTER COLUMN p_registration_end TYPE date USING p_registration_end::date,
	ALTER COLUMN p_build_year TYPE integer USING p_build_year::integer;