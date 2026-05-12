# ==========================================
# INTERNAL PLANTEL MAPPING
# ==========================================
#
# Each plantel can have different identifiers depending on the subsystem.
# Keep the legacy DB/API codes here instead of leaking those quirks into every
# repository. The top-level db_code/sheets_code keys are kept for backwards
# compatibility with the existing services.

PLANTEL_MAP = {
    "PM": {
        "db_code": "PM",
        "db_codes": ["PM"],
        "sheets_code": "PM",
        "sheets_codes": ["PM"],
        "name": "Primaria Metepec",
        "academic_filters": [{"nivel": "Primaria", "campus": "Metepec"}],
        "sapf_map_campus": "PM",
        "sapf_data_campuses": ["PMA", "PMB"],
    },
    "PMA": {
        "alias_of": "PM",
        "db_code": "PM",
        "db_codes": ["PM"],
        "sheets_code": "PM",
        "sheets_codes": ["PM"],
        "name": "Primaria Metepec",
        "academic_filters": [{"nivel": "Primaria", "campus": "Metepec"}],
        "sapf_map_campus": "PM",
        "sapf_data_campuses": ["PMA", "PMB"],
    },
    "PMB": {
        "alias_of": "PM",
        "db_code": "PM",
        "db_codes": ["PM"],
        "sheets_code": "PM",
        "sheets_codes": ["PM"],
        "name": "Primaria Metepec",
        "academic_filters": [{"nivel": "Primaria", "campus": "Metepec"}],
        "sapf_map_campus": "PM",
        "sapf_data_campuses": ["PMA", "PMB"],
    },

    "PT": {
        "db_code": "PT",
        "db_codes": ["PT"],
        "sheets_code": "PT",
        "sheets_codes": ["PT"],
        "name": "Primaria Toluca",
        "academic_filters": [{"nivel": "Primaria", "campus": "Toluca"}],
        "sapf_map_campus": "PT",
        "sapf_data_campuses": ["PT"],
    },
    "SM": {
        "db_code": "SM",
        "db_codes": ["SM"],
        "sheets_code": "SM",
        "sheets_codes": ["SM"],
        "name": "Secundaria Metepec",
        "academic_filters": [{"nivel": "Secundaria", "campus": "Metepec"}],
        "sapf_map_campus": "SM",
        "sapf_data_campuses": ["SM"],
    },
    "ST": {
        "db_code": "ST",
        "db_codes": ["ST"],
        "sheets_code": "ST",
        "sheets_codes": ["ST"],
        "name": "Secundaria Toluca",
        "academic_filters": [{"nivel": "Secundaria", "campus": "Toluca"}],
        "sapf_map_campus": "ST",
        "sapf_data_campuses": ["ST"],
    },

    "PREET": {
        "db_code": "PREET",
        "db_codes": ["PREET"],
        "sheets_code": "PREET",
        "sheets_codes": ["PREET"],
        "name": "PREET",
        "display_name": "Preescolar Toluca (PREET)",
        "academic_filters": [{"nivel": "Preescolar", "campus": "Toluca"}],
        "sapf_map_campus": "PREET",
        "sapf_data_campuses": ["PREET"],
    },
    "CT": {
        "alias_of": "PREET",
        "db_code": "PREET",
        "db_codes": ["PREET"],
        "sheets_code": "PREET",
        "sheets_codes": ["PREET"],
        "name": "PREET",
        "display_name": "Preescolar Toluca (PREET)",
        "academic_filters": [{"nivel": "Preescolar", "campus": "Toluca"}],
        "sapf_map_campus": "PREET",
        "sapf_data_campuses": ["PREET"],
    },

    "PREEM": {
        "db_code": "PREEM",
        "db_codes": ["PREEM"],
        "sheets_code": "PREEM",
        "sheets_codes": ["PREEM"],
        "name": "PREEM",
        "display_name": "Preescolar Metepec (PREEM)",
        "academic_filters": [{"nivel": "Preescolar", "campus": "Metepec"}],
        "sapf_map_campus": "PREEM",
        "sapf_data_campuses": ["PREEM"],
    },
    "CM": {
        "alias_of": "PREEM",
        "db_code": "PREEM",
        "db_codes": ["PREEM"],
        "sheets_code": "PREEM",
        "sheets_codes": ["PREEM"],
        "name": "PREEM",
        "display_name": "Preescolar Metepec (PREEM)",
        "academic_filters": [{"nivel": "Preescolar", "campus": "Metepec"}],
        "sapf_map_campus": "PREEM",
        "sapf_data_campuses": ["PREEM"],
    },

    # Other plantels that can appear in upstream tools. They remain standalone
    # and are never merged into PREEM.
    "DM": {
        "db_code": "DM",
        "db_codes": ["DM"],
        "sheets_code": "DM",
        "sheets_codes": ["DM"],
        "name": "Desarrollo Metepec",
        "academic_filters": [],
        "sapf_map_campus": "DM",
        "sapf_data_campuses": ["DM"],
    },
    "PR": {
        "db_code": "PR",
        "db_codes": ["PR"],
        "sheets_code": "PR",
        "sheets_codes": ["PR"],
        "name": "Preescolar / PR",
        "academic_filters": [],
        "sapf_map_campus": "PR",
        "sapf_data_campuses": ["PR"],
    },

    "01": {
        "db_code": "01",
        "db_codes": ["01"],
        "sheets_code": "PT",
        "sheets_codes": ["PT"],
        "name": "Primaria Toluca (01)",
        "academic_filters": [{"nivel": "Primaria", "campus": "Toluca"}],
        "sapf_map_campus": "PT",
        "sapf_data_campuses": ["PT"],
    },
}


# Public plantels used by cache pre-warming and global comparisons.
ACTIVE_PLANTEL_CODES = ["PM", "PT", "SM", "ST", "PREET", "PREEM"]
