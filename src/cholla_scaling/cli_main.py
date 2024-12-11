import argparse
import os

from .problem_props import (
    build_cases, OriginLoc, ProblemCase, ProblemRegistry, setup_problem_dir
)

parser = argparse.ArgumentParser(
    description="assists with scaling tests"
)
parser.add_argument(
    "--cholla-dir", required=True, help="path to the cholla directory"
)
parser.add_argument(
    "--host", required=True, help="the name of the host"
)
parser.add_argument(
    "--test-problem",
    required=True,
    choices=list(ProblemRegistry.keys()),
    nargs='+',
    help="the name of the test"
)
parser.add_argument(
    "--test-dir",
    default='.',
    help="path to the directory where we will put the tests"
)

parser.add_argument(
    "--max_nproc",
    required=True,
    type=int,
    help="max number of processes"
)

make_job_grp = parser.add_mutually_exclusive_group()
make_job_grp.add_argument(
    "--make-jflag-none", action="store_const", dest="make_jobs", const=False
)
make_job_grp.add_argument(
    "--make-jflag-max", action="store_const", dest="make_jobs", const=True
)
make_job_grp.add_argument(
    "--make-jflag", action="store", type=int, dest="make_jobs"
)



def cli_main(override_args=None):
    args = parser.parse_args(override_args)

    make_jobs = getattr(args,'make_jobs', None)
    if make_jobs is None:
        make_jobs = False

    cholla_dir = args.cholla_dir
    assert os.path.isdir(cholla_dir)
    hostname = args.host

    max_nproc = args.max_nproc
    assert max_nproc > 0

    base_dir = args.test_dir
    if not os.path.isdir(base_dir):
        os.mkdir(base_dir)

    test_pairs = [
        (os.path.join(base_dir, name), ProblemRegistry[name])
        for name in sorted(set(args.test_problem))
    ]

    for path,_ in filter(lambda p: os.path.exists(p[0]), test_pairs):
        raise ValueError(f"the path `{path}` already exists")

    for path, problem_prop in test_pairs:
        print(path)
        os.mkdir(path)


        if problem_prop.origin_loc == OriginLoc.LEFT:
            grid_left_xyz=(0.0, 0.0, 0.0)
            grid_width_xyz=(2.0, 2.0, 2.0)
        elif problem_prop.origin_loc == OriginLoc.CENTER:
            grid_left_xyz=(-4.0, -4.0, -4.0)
            grid_width_xyz=(8.0, 8.0, 8.0)
        else:
            raise RuntimeError()


        base_problem_case = ProblemCase(
            grid_left_xyz=grid_left_xyz,
            grid_width_xyz=grid_width_xyz,
            grid_shape_xyz=(350,350,350),
            nproc_grid_xyz=(1,1,1) 
        )

        case_itr = build_cases(
            base_problem_case,
            origin_loc=problem_prop.origin_loc,
            scale_rule=problem_prop.scale_rule,
            max_proc=args.max_nproc,
        )
        case_l = list(case_itr)
        assert len(case_l) > 0

        setup_problem_dir(
            path, cholla_dir=cholla_dir, problem_props = problem_prop,
            problem_cases = case_l, hostname = hostname, make_jobs=make_jobs
        )

    return 0
