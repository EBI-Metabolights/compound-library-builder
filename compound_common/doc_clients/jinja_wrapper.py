from jinja2 import Environment, FileSystemLoader


class JinjaWrapper:
    """
    Basic wrapper around pythons jinja2 library.
    """

    def __init__(self):
        self.env = Environment(loader=FileSystemLoader("../../templates"))
        self.template = None

    def load_template(self, template_path):
        """
        Load a given jinja template.
        :param template_path: Path to given template
        :return: Loaded jinja template.
        """
        self.template = self.env.get_template(template_path)

    def render_template(self, variables_yaml):
        """
        Dump a set of cariables into the loaded template.
        :param variables_yaml:
        :return: Rendered template object.
        """
        return self.template.render(variables_yaml)
