import xml.etree.ElementTree as ET


class XmlResponseUtils:
    """
    Collection of static XML utility methods.
    """

    @staticmethod
    def convert_to_element(response_text) -> ET.Element:
        """
        Convert a string to an ET.Element object
        :param response_text: string to convert
        :return: Element object.
        """
        return ET.fromstring(response_text)

    @staticmethod
    def get_chebi_id(response_text) -> str:
        """
        Extract a ChEBI ID from a stringified version of an XML response. Chain calls Element.find until it finds the
        chebiID element (or not).
        :param response_text: Stringified version of XML response.
        :return: ChEBI ID
        """
        chebi_ns_map: dict = {
            "envelop": "http://schemas.xmlsoap.org/soap/envelope/",
            "chebi": "{http://www.ebi.ac.uk/webservices/chebi}",
        }
        id = None
        try:
            root = (
                ET.fromstring(response_text)
                .find("envelop:Body", namespaces=chebi_ns_map)
                .find(
                    "{https://www.ebi.ac.uk/webservices/chebi}getCompleteEntityResponse"
                )
                .find("{https://www.ebi.ac.uk/webservices/chebi}return")
            )

            id = root.find("{https://www.ebi.ac.uk/webservices/chebi}chebiId").text
        except ET.ParseError as e:
            print(f"XML parsing error occurred: {str(e)}")
        except AttributeError as e:
            print(f"Attribute error while calling .find on xml document: {str(e)}")
        return id

    @staticmethod
    def element_to_dict(element) -> dict:
        """
        Return an xml element as a dict, including all children. This method is 'stupid' and assumes a valid XML
        element.
        :param element: XML element.
        :return: dict representation of XML element.
        """
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
