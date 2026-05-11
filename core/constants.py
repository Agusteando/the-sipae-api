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

    # Legacy CT is still the operational code for PREET in most subsystems.
    "CT": {
        "db_code": "CT",
        "db_codes": ["CT"],
        "sheets_code": "CT",
        "sheets_codes": ["CT"],
        "name": "PREET",
        "display_name": "Preescolar Toluca (PREET)",
        "academic_filters": [],
        "sapf_map_campus": "CT",
        "sapf_data_campuses": ["CT"],
    },
    "PREET": {
        "alias_of": "CT",
        "db_code": "CT",
        "db_codes": ["CT"],
        "sheets_code": "CT",
        "sheets_codes": ["CT"],
        "name": "PREET",
        "display_name": "Preescolar Toluca (PREET)",
        "academic_filters": [],
        "sapf_map_campus": "CT",
        "sapf_data_campuses": ["CT"],
    },

    # PREEM is less consistent across apps. CM remains the default operational
    # code, while SAPF can have records under CM, DM and PR.
    "CM": {
        "db_code": "CM",
        "db_codes": ["CM"],
        "sheets_code": "CM",
        "sheets_codes": ["CM"],
        "name": "PREEM",
        "display_name": "Preescolar Metepec (PREEM)",
        "academic_filters": [],
        "sapf_map_campus": "CM",
        "sapf_data_campuses": ["CM", "DM", "PR"],
    },
    "PREEM": {
        "alias_of": "CM",
        "db_code": "CM",
        "db_codes": ["CM"],
        "sheets_code": "CM",
        "sheets_codes": ["CM"],
        "name": "PREEM",
        "display_name": "Preescolar Metepec (PREEM)",
        "academic_filters": [],
        "sapf_map_campus": "CM",
        "sapf_data_campuses": ["CM", "DM", "PR"],
    },

    # Other legacy aliases that can appear in upstream tools.
    "DM": {
        "db_code": "DM",
        "db_codes": ["DM"],
        "sheets_code": "DM",
        "sheets_codes": ["DM"],
        "name": "Desarrollo Metepec",
        "academic_filters": [],
        "sapf_map_campus": "CM",
        "sapf_data_campuses": ["DM"],
    },
    "PR": {
        "db_code": "PR",
        "db_codes": ["PR"],
        "sheets_code": "PR",
        "sheets_codes": ["PR"],
        "name": "Preescolar / PR",
        "academic_filters": [],
        "sapf_map_campus": "CM",
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


# Public plantels used by cache pre-warming and global comparisons. Aliases such
# as CT/CM remain accepted by endpoints but are not precomputed twice.
ACTIVE_PLANTEL_CODES = ["PM", "PT", "SM", "ST", "PREET", "PREEM"]
