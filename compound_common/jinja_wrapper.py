from jinja2 import Environment, FileSystemLoader


class JinjaWrapper:

    def __init__(self):
        self.env = Environment(loader=FileSystemLoader('../templates'))
        self.template = None

    def load_template(self, template_path):
        self.template = self.env.get_template(template_path)

    def render_template(self, variables_yaml):
        return self.template.render(variables_yaml)

