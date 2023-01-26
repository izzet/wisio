def print_header(header: str, tab_indent=0):
    line = f"{'='*100}"
    print_tabbed(line, tab_indent=tab_indent)
    print_tabbed(header, tab_indent=tab_indent)
    print_tabbed(line, tab_indent=tab_indent)


def print_tabbed(message: str, tab_indent=0):
    print('\t'*tab_indent + message)
