{{
    config(
        materialized = 'view'
    )
}}

SELECT
    CAST(GET(XMLGET($1, 'Customer'), '@C_ID') AS BIGINT) AS customerid,
    CAST(GET(
        XMLGET(XMLGET($1, 'Customer'), 'Account'), '@CA_ID'
    ) AS BIGINT) AS accountid,
    CAST(GET(
        XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Account'), 'CA_B_ID'), '$'
    ) AS BIGINT) AS brokerid,
    CAST(NULLIF(
        GET(XMLGET($1, 'Customer'), '@C_TAX_ID'), ''
    ) AS STRING) AS taxid,
    CAST(NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'Account'),
                'CA_NAME'
            ),
            '$'
        ),
        ''
    ) AS STRING) AS accountdesc,
    CAST(GET(
        XMLGET(XMLGET($1, 'Customer'), 'Account'), '@CA_TAX_ST'
    ) AS TINYINT) AS taxstatus,
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
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Name'), 'C_L_NAME'),
            '$'
        ),
        ''
    ) AS STRING) AS lastname,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Name'), 'C_F_NAME'),
            '$'
        ),
        ''
    ) AS STRING) AS firstname,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Name'), 'C_M_NAME'),
            '$'
        ),
        ''
    ) AS STRING) AS middleinitial,
    NULLIF(UPPER(GET(XMLGET($1, 'Customer'), '@C_GNDR')), '') AS gender,
    CAST(NULLIF(GET(XMLGET($1, 'Customer'), '@C_TIER'), '') AS STRING) AS tier,
    CAST(GET(XMLGET($1, 'Customer'), '@C_DOB') AS DATE) AS dob,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_ADLINE1'), '$'
        ),
        ''
    ) AS STRING) AS addressline1,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_ADLINE2'),
            '$'
        ),
        ''
    ) AS STRING) AS addressline2,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_ZIPCODE'),
            '$'
        ),
        ''
    ) AS STRING) AS postalcode,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_CITY'),
            '$'
        ),
        ''
    ) AS STRING) AS city,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_STATE_PROV'),
            '$'
        ),
        ''
    ) AS STRING) AS stateprov,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'Address'), 'C_CTRY'),
            '$'
        ),
        ''
    ) AS STRING) AS country,
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
        CAST(NULL AS STRING)
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
        CAST(NULL AS STRING)
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
        CAST(NULL AS STRING)
    ) AS phone3,
    CAST(NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                'C_PRIM_EMAIL'
            ),
            '$'
        ),
        ''
    ) AS STRING) AS email1,
    CAST(NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'ContactInfo'),
                'C_ALT_EMAIL'
            ),
            '$'
        ),
        ''
    ) AS STRING) AS email2,
    CAST(NULLIF(
        GET(
            XMLGET(XMLGET(XMLGET($1, 'Customer'), 'TaxInfo'), 'C_LCL_TX_ID'),
            '$'
        ),
        ''
    ) AS STRING) AS lcl_tx_id,
    CAST(NULLIF(
        GET(
            XMLGET(
                XMLGET(XMLGET($1, 'Customer'), 'TaxInfo'),
                'C_NAT_TX_ID'
            ),
            '$'
        ),
        ''
    ) AS STRING) AS nat_tx_id,
    TO_TIMESTAMP(GET($1, '@ActionTS')) AS update_ts,
    CAST(GET($1, '@ActionType') AS STRING) AS actiontype
FROM
    @{{ var('stage') }}/Batch1 (
        FILE_FORMAT => 'XML', PATTERN => '.*CustomerMgmt[.]xml.*'
    )
