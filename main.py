import sys
from FindCitations import FindCitations
from FindCyclicReferences import FindCyclicReferences


def parse_arguments(args: list) -> dict:
    parsed_args = {}
    subclass = args[0]
    if subclass not in ["COUNT", "CYCLE"]:
        raise Exception("Subclass must be COUNT or CYCLE")
    parsed_args["subclass"] = subclass
    parsed_args["num_workers"] = int(args[1])
    if (workers := parsed_args["num_workers"]) < 1 or workers > 10:
        raise Exception("Number of workers must be between 1 and 10")
    parsed_args["input_file"] = args[2]
    return parsed_args


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    if args["subclass"] == "COUNT":
        instance = FindCitations(args["num_workers"])
    else:
        instance = FindCyclicReferences(args["num_workers"])

    instance.start(args["input_file"])
