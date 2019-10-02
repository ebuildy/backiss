import argparse, json, re, os

parser = argparse.ArgumentParser(description='Backup as a kiss!')
parser.add_argument('action', type=str, help='backup / restore / status')
parser.add_argument('--rules', type=str, help='rule definition (JSON format)', nargs="*")
parser.add_argument('--dry-run', help='dont do anything really', action="store_true")
parser.add_argument('--year', help='optional year')
parser.add_argument('--month', help='optional year')
parser.add_argument('--day', help='optional year')
parser.add_argument("-v", "--verbose", help="increase output verbosity", action="store_true")

args = parser.parse_args()

def main(args):

    def remove_duplicate_slash(p):
        return re.compile(r"(\/)\1{1,}").sub(r"\1", p)

    def do_copy(source_file_system, source_path, target_file_system, target_path):
        real_target_path = os.path.dirname(target_path)

        if target_file_system == "file" and not os.path.exists(real_target_path):
            if args.verbose:
                print(f"\t\tmkdir(\"{real_target_path}\")")
            if not args.dry_run:
                os.makedirs(real_target_path)

        if not args.dry_run:
            if source_file_system == "hdfs" and target_file_system == "file":
                str_exec = f"hdfs dfs -get {source_path} {real_target_path}/"
                print(f"{str_exec}")
                os.system(str_exec)
                print("")
                return 'ok'
            else:
                return 'nop'

    # Build the path_date_suffix
    path_date = ''

    if args.year:
        path_date = f'/year={args.year}'

    if args.month:
        path_date = f'{path_date}/month={args.month}'

    if args.day:
        path_date = f'{path_date}/day={args.day}'

    if args.verbose:
        print(f'path_date = "{path_date}"')

    for rule_path in args.rules:

        if args.verbose:
            print("doing " + rule_path)

        with open(rule_path, "r") as file:
            rule = json.loads(file.read())

            for source_path in rule['source']['paths']:

                source_path = source_path.format_map(locals())
                source_path = remove_duplicate_slash(source_path)
                dest_path = source_path

                if 'rewrite' in rule:
                    for k, v in rule['rewrite'].items():
                        dest_path = re.compile(k).sub(v, source_path)

                dest_path = remove_duplicate_slash(dest_path)

                if args.action == "backup":
                    do_copy(rule['source']['file_system'],
                            source_path,
                            rule['target']['file_system'],
                            dest_path)
                elif args.action == "status":
                    print(f"Status of {dest_path}:")
                    os.system(f"du -hs {dest_path}/*")
                    print("")


if __name__== "__main__" :
    main(args)
