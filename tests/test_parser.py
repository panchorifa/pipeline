from lib import parser

SAMPLE_CSV_HEADER = \
    'NPI,Provider Last Name (Legal Name),Provider First Name,Provider Middle Name,Provider Name Prefix Text,Provider Name Suffix Text,Provider Credential Text,Provider Other Last Name,Provider Other First Name,Provider Other Middle Name,Provider Other Name Prefix Text,Provider Other Name Suffix Text,Provider Other Credential Text,Provider Other Last Name Type Code,Provider First Line Business Mailing Address,Provider Second Line Business Mailing Address,Provider Business Mailing Address City Name,Provider Business Mailing Address State Name,Provider Business Mailing Address Postal Code,Provider Business Mailing Address Country Code (If outside U.S.),Provider Business Mailing Address Telephone Number,Provider Business Mailing Address Fax Number,Provider Gender Code,Healthcare Provider Taxonomy Code_1,Healthcare Provider Taxonomy Code_2,Healthcare Provider Taxonomy Code_3'

SAMPLE_CSV_ENTRY = \
    '1003000167,ESCOBAR,JULIO,EDGARDO,DR.,NULL,DDS,NULL,NULL,NULL,NULL,NULL,NULL,NULL,5 PINE CONE RD,NULL,DAYTON,NV,894037482,US,7752207788,NULL,M,122300000X,NULL,NULL'

def test_parse_header():
    values = parser.parse_header(SAMPLE_CSV_HEADER)
    assert len(values) == 26
    assert values[0] == 'npi'
    assert values[1] == 'provider_last_name'
    assert values[2] == 'provider_first_name'

def test_parse_data():
    values = parser.parse_data(SAMPLE_CSV_ENTRY)
    assert len(values) == 26
    assert values[0] == '1003000167'
    assert values[1] == 'ESCOBAR'
    assert values[2] == 'JULIO'
    assert values[3] == 'EDGARDO'
    assert values[23] == '122300000X'
    assert values[24] == None
    assert values[25] == None
