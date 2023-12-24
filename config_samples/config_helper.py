import argparse
import yaml


def set_value(value):
    if value.isdigit():
        return int(value)
    elif value.replace(".", "").isdigit():
        return float(value)
    elif value in ['True', 'true']:
        return True
    elif value in ['False', 'false']:
        return False
    else:
        return value


def get(my_dict, key_string):
    here = my_dict
    keys = key_string.split(".")
    for key in keys[:-1]:
        here = here.setdefault(key, {})
    return here[keys[-1]]


def set(my_dict, key_string, value):
    here = my_dict
    keys = key_string.split(".")
    for key in keys[:-1]:
        here = here.setdefault(key, {})
    here[keys[-1]] = set_value(value)


def main():

    parser = argparse.ArgumentParser()

    base_parser = argparse.ArgumentParser(add_help=False)
    base_parser.add_argument('path')
    base_parser.add_argument('key')

    subparsers = parser.add_subparsers(title='commands', dest='command')

    get_parser = subparsers.add_parser('get', parents=[base_parser])
    set_parser = subparsers.add_parser('set', parents=[base_parser])
    set_parser.add_argument('value')

    args = parser.parse_args()

    with open(args.path, 'r') as file:
        config = yaml.safe_load(file)

    if args.command == 'get':
        print(get(config, args.key))
    elif args.command == 'set':
        print('Setting', args.key, 'to', args.value, 'for', args.path)
        set(config, args.key, args.value)
        with open(args.path, 'w') as file:
            yaml.safe_dump(config, file)


if __name__ == '__main__':
    main()
