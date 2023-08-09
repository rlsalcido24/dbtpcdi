{{
    config(
        materialized = 'view'
    )
}}

SELECT
    GET(XMLGET($1, 'Customer'), '@C_ID')::BIGINT AS customerid,
    GET(
        XMLGET(XMLGET($1, 'Customer'), 'Account'), '@CA_ID'
    )::BIGINT AS accountid,
    GET(
        XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Account'), 'CA_B_ID'), '$'
    )::BIGINT AS brokerid,
    NULLIF(
        GET(XMLGET($1, 'Customer'), '@C_TAX_ID'), ''
    )::STRING AS taxid,
    NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'Account'),
                'CA_NAME'
            ),
            '$'
        ),
        ''
    )::STRING AS accountdesc,
    GET(
        XMLGET(XMLGET($1, 'Customer'), 'Account'), '@CA_TAX_ST'
    )::TINYINT AS taxstatus,
    DECODE(
        GET($1, '@ActionType'),
        'NEW',
        'Active',
        'ADDACCT',
        'Active',
        'UPDACCT',
        'Active',
        'UPDCUST',
        'Active',
        'CLOSEACCT',
        'Inactive',
        'INACT',
        'Inactive'
    ) AS status,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Name'), 'C_L_NAME'),
            '$'
        ),
        ''
    )::STRING AS lastname,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Name'), 'C_F_NAME'),
            '$'
        ),
        ''
    )::STRING AS firstname,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Name'), 'C_M_NAME'),
            '$'
        ),
        ''
    )::STRING AS middleinitial,
    NULLIF(UPPER(GET(XMLGET($1, 'Customer'), '@C_GNDR')), '') AS gender,
    NULLIF(GET(XMLGET($1, 'Customer'), '@C_TIER'), '')::STRING AS tier,
    GET(XMLGET($1, 'Customer'), '@C_DOB')::DATE AS dob,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_ADLINE1'), '$'
        ),
        ''
    )::STRING AS addressline1,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_ADLINE2'),
            '$'
        ),
        ''
    )::STRING AS addressline2,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_ZIPCODE'),
            '$'
        ),
        ''
    )::STRING AS postalcode,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_CITY'),
            '$'
        ),
        ''
    )::STRING AS city,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_STATE_PROV'),
            '$'
        ),
        ''
    )::STRING AS stateprov,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_CTRY'),
            '$'
        ),
        ''
    )::STRING AS country,
    NVL2(
        NULLIF(
            GET(
                XMLGET(
                    XMLGET(
                        XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                        'C_PHONE_1'
                    ),
                    'C_LOCAL'
                ),
                '$'
            ),
            ''
        ),
        CONCAT(
            NVL2(
                NULLIF(
                    GET(
                        XMLGET(
                            XMLGET(
                                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                                'C_PHONE_1'
                            ),
                            'C_CTRY_CODE'
                        ),
                        '$'
                    ),
                    ''
                ),
                '+' || GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_1'
                        ),
                        'CTRY_CODE'
                    ),
                    '$'
                ) || ' ',
                ''
            ),
            NVL2(
                NULLIF(
                    GET(
                        XMLGET(
                            XMLGET(
                                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                                'C_PHONE_1'
                            ),
                            'C_AREA_CODE'
                        ),
                        '$'
                    ),
                    ''
                ),
                '(' || GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_1'
                        ),
                        'C_AREA_CODE'
                    ),
                    '$'
                ) || ') ',
                ''
            ),
            GET(
                XMLGET(
                    XMLGET(
                        XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                        'C_PHONE_1'
                    ),
                    'C_LOCAL'
                ),
                '$'
            ),
            COALESCE(
                GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_1'
                        ),
                        'C_EXT'
                    ),
                    '$'
                ),
                ''
            )
        ),
        CAST(null AS STRING)
    ) AS phone1,
    NVL2(
        NULLIF(
            GET(
                XMLGET(
                    XMLGET(
                        XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                        'C_PHONE_2'
                    ),
                    'C_LOCAL'
                ),
                '$'
            ),
            ''
        ),
        CONCAT(
            NVL2(
                NULLIF(
                    GET(
                        XMLGET(
                            XMLGET(
                                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                                'C_PHONE_2'
                            ),
                            'C_CTRY_CODE'
                        ),
                        '$'
                    ),
                    ''
                ),
                '+' || GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_2'
                        ),
                        'CTRY_CODE'
                    ),
                    '$'
                ) || ' ',
                ''
            ),
            NVL2(
                NULLIF(
                    GET(
                        XMLGET(
                            XMLGET(
                                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                                'C_PHONE_2'
                            ),
                            'C_AREA_CODE'
                        ),
                        '$'
                    ),
                    ''
                ),
                '(' || GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_2'
                        ),
                        'C_AREA_CODE'
                    ),
                    '$'
                ) || ') ',
                ''
            ),
            GET(
                XMLGET(
                    XMLGET(
                        XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                        'C_PHONE_2'
                    ),
                    'C_LOCAL'
                ),
                '$'
            ),
            COALESCE(
                GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_2'
                        ),
                        'C_EXT'
                    ),
                    '$'
                ),
                ''
            )
        ),
        CAST(null AS STRING)
    ) AS phone2,
    NVL2(
        NULLIF(
            GET(
                XMLGET(
                    XMLGET(
                        XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                        'C_PHONE_3'
                    ),
                    'C_LOCAL'
                ),
                '$'
            ),
            ''
        ),
        CONCAT(
            NVL2(
                NULLIF(
                    GET(
                        XMLGET(
                            XMLGET(
                                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                                'C_PHONE_3'
                            ),
                            'C_CTRY_CODE'
                        ),
                        '$'
                    ),
                    ''
                ),
                '+' || GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_3'
                        ),
                        'CTRY_CODE'
                    ),
                    '$'
                ) || ' ',
                ''
            ),
            NVL2(
                NULLIF(
                    GET(
                        XMLGET(
                            XMLGET(
                                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                                'C_PHONE_3'
                            ),
                            'C_AREA_CODE'
                        ),
                        '$'
                    ),
                    ''
                ),
                '(' || GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_3'
                        ),
                        'C_AREA_CODE'
                    ),
                    '$'
                ) || ') ',
                ''
            ),
            GET(
                XMLGET(
                    XMLGET(
                        XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                        'C_PHONE_3'
                    ),
                    'C_LOCAL'
                ),
                '$'
            ),
            COALESCE(
                GET(
                    XMLGET(
                        XMLGET(
                            XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                            'C_PHONE_3'
                        ),
                        'C_EXT'
                    ),
                    '$'
                ),
                ''
            )
        ),
        CAST(null AS STRING)
    ) AS phone3,
    NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                'C_PRIM_EMAIL'
            ),
            '$'
        ),
        ''
    )::STRING AS email1,
    NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                'C_ALT_EMAIL'
            ),
            '$'
        ),
        ''
    )::STRING AS email2,
    NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'TaxInfo'), 'C_LCL_TX_ID'),
            '$'
        ),
        ''
    )::STRING AS lcl_tx_id,
    NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'TaxInfo'),
                'C_NAT_TX_ID'
            ),
            '$'
        ),
        ''
    )::STRING AS nat_tx_id,
    TO_TIMESTAMP(GET($1, '@ActionTS')) AS update_ts,
    GET($1, '@ActionType')::STRING AS actiontype
FROM
    @{{ var('stage') }}/Batch1 (
        FILE_FORMAT => 'XML', PATTERN => '.*CustomerMgmt[.]xml.*'
    )
