import xml.etree.ElementTree as ET

class DataExtractor:
    def __init__(self, root, ns):
        self.root = root
        self.ns = ns

    def get_text_or_none(self, element, xpath):
        found_element = element.find(xpath, namespaces=self.ns)
        return found_element.text if found_element is not None else None

    def extract_vbo_data(self):
        return self._extract_common_data(
            './/ns2:Verblijfsobject',
            {
                'id_vbo': './/ns2:identificatie',
                'id_num': './/ns3:NummeraanduidingRef',
                'id_pand': './/ns3:PandRef',
                'geometry': './/ns5:pos',
                'function': './/ns2:gebruiksdoel',
                'sqm': './/ns2:oppervlakte',
                'status': './/ns2:status',
                'document_date': './/ns2:documentdatum',
                'document_number': './/ns2:documentnummer',
                'registration_start': './/ns4:Voorkomen/ns4:beginGeldigheid',
                'registration_end': './/ns4:Voorkomen/ns4:eindGeldigheid'
            }
        )

    def extract_pand_data(self):
        return self._extract_common_data(
            './/lvbag:Pand',
            {
                'id_pand': './/lvbag:identificatie',
                'geometry': './/gml:posList',
                'build_year': './/lvbag:oorspronkelijkBouwjaar',
                'status': './/lvbag:status',
                'document_date': './/lvbag:documentdatum',
                'document_number': './/lvbag:documentnummer',
                'registration_start': './/hist:beginGeldigheid',
                'registration_end': './/hist:eindGeldigheid'
            }
        )

    def extract_num_data(self):
        return self._extract_common_data(
            './/lvbag:Nummeraanduiding',
            {
                'id_num': 'lvbag:identificatie',
                'house_number': 'lvbag:huisnummer',
                'house_letter': 'lvbag:huisletter',
                'post_code': 'lvbag:postcode',
                'document_date': 'lvbag:documentdatum',
                'document_number': 'lvbag:documentnummer',
                'registration_start': './/hist:beginGeldigheid',
                'registration_end': './/hist:eindGeldigheid'
            }
        )

    def _extract_common_data(self, xpath, fields):
        data = []
        elements = self.root.findall(xpath, namespaces=self.ns)
        for elem in elements:
            extracted_data = tuple(self.get_text_or_none(elem, path) for path in fields.values())
            data.append(extracted_data)
        return data
