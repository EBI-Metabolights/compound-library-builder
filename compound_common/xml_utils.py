import xml.etree.ElementTree as ET


class XmlResponseUtils:

    @staticmethod
    def convert_to_element(response_text):
        return ET.fromstring(response_text)

    @staticmethod
    def get_chebi_id(response_text):
        chebi_ns_map: dict = {
            "envelop": "http://schemas.xmlsoap.org/soap/envelope/",
            "chebi": "{http://www.ebi.ac.uk/webservices/chebi}"
        }
        id = None
        try:
            root = ET.fromstring(response_text) \
            .find("envelop:Body", namespaces=chebi_ns_map) \
            .find("{https://www.ebi.ac.uk/webservices/chebi}getCompleteEntityResponse") \
            .find("{https://www.ebi.ac.uk/webservices/chebi}return")

            id = root.find("{https://www.ebi.ac.uk/webservices/chebi}chebiId").text
        except ET.ParseError as e:
            print(f'XML parsing error occurred: {str(e)}')
        except AttributeError as e:
            print(f'Attribute error while calling .find on xml document: {str(e)}')
        return id

    @staticmethod
    def element_to_dict(element):
        if len(element) == 0:
            return element.text
        result = {}
        for child in element:
            child_data = XmlResponseUtils.element_to_dict(child)
            if child.tag in result:
                if type(result[child.tag]) is list:
                    result[child.tag].append(child_data)
                else:
                    result[child.tag] = [result[child.tag], child_data]
            else:
                result[child.tag] = child_data
        return result



