from enum import Enum, auto
import functools
import os
import shutil
import subprocess
from typing import NamedTuple, Tuple, Union

from .pretty_subprocess import pretty_subprocess_run

_LOCAL_DIR = os.path.dirname(__file__)
_MAKETYPE_PREFIX = 'make.type.'

def _prod(iterable, *, start=1):
    out=start
    for elem in iterable:
        out*=elem
    return out


class ProblemCase(NamedTuple):
    """
    Represents a single occurence of a problem
    """
    # left edge of the grid in code units
    grid_left_xyz: Tuple[float, float, float]
    # total width of the grid in code units
    grid_width_xyz: Tuple[float, float, float]
    # specifies full shape of the grid (excluding ghost zones)
    grid_shape_xyz: Tuple[int, int, int]
    # number of blocks (subgrids) along each axis
    nproc_grid_xyz: Tuple[int, int, int]

    def total_proc(self):
        return _prod(self.nproc_grid_xyz)


class OriginLoc(Enum):
    LEFT = auto()
    CENTER = auto()

class ScaleRule(Enum):
    # can increment x-axis (while holding other axes constant)
    X_AXIS=auto()
    # can increment z-axis (while holding other axes constant)
    Z_AXIS=auto()
    # can increment xy-plane (while holding z-axis constant)
    XY_PLANE=auto()

class ProblemProps(NamedTuple):
    name: str
    origin_loc: OriginLoc
    scale_rule: ScaleRule

    @property
    def inputs_loc(self):
        return os.path.join(_LOCAL_DIR, self.name)

    def maketype_and_param_paths(self):
        maketype_path, param_path = None, None
        inputs_loc = self.inputs_loc

        with os.scandir(inputs_loc) as it:
            count = 0
            for entry in filter(lambda entry: entry.is_file(), it):
                count += 1
                if count == 3:
                    break
                elif entry.name.startswith(_MAKETYPE_PREFIX):
                    maketype_path = entry.path
                else:
                    param_path = entry.path
        if count != 2:
            raise RuntimeError(
                f"we expect `{inputs_loc}` to contain exactly 2 files"
            )
        elif maketype_path is None:
            raise RuntimeError("no file looks like a make.type. file")
        elif param_path is None:
            raise RuntimeError("all files look like a make.type. file")
        return maketype_path, param_path

def _next_case(case: ProblemCase, origin_loc: OriginLoc, scale_rule: ScaleRule):
    if scale_rule == ScaleRule.X_AXIS:
        def generic_transform(triple):
            return (triple[0]*2, triple[1], triple[2])
    elif scale_rule == ScaleRule.Z_AXIS:
        def generic_transform(triple):
            return (triple[0], triple[1], triple[2]*2)
    elif scale_rule == ScaleRule.XY_AXIS:
        def generic_transform(triple):
            return (triple[0]*2, triple[2]*2, triple[2])
    else:
        raise RuntimeError(f"missing branch for {scale_rule}")

    # apply generic_transform to each "generic_field" and store in a dict
    generic_fields = ["grid_width_xyz", "grid_shape_xyz", "nproc_grid_xyz"]
    kwargs = {f: generic_transform(getattr(case,f)) for f in generic_fields}

    if origin_loc == OriginLoc.LEFT:
        assert all(v==0 for v in case.grid_left_xyz)
        kwargs["grid_left_xyz"] = case.grid_left_xyz
    elif origin_loc == OriginLoc.CENTER:
        kwargs["grid_left_xyz"] = generic_transform(case.grid_left_xyz)
    else:
        raise RuntimeError(f"missing branch for {origin_rule}")

    return ProblemCase(**kwargs)

def build_cases(base_case: ProblemCase,
                origin_loc: OriginLoc,
                scale_rule: ScaleRule,
                max_proc: int,
                min_generated_proc: int = 1,
                include_base_case: bool = True):
    """
    Iterate over all allowed cases
    """
    assert 1 <= max_proc
    assert 0 <= min_generated_proc <= max_proc
    assert base_case.total_proc() <= max_proc

    if include_base_case:
        yield base_case
    case = base_case

    while True:
        case = _next_case(
            case=case, origin_loc=origin_loc, scale_rule=scale_rule
        )
        total_proc = case.total_proc()
        if total_proc < min_generated_proc:
            continue
        elif total_proc > max_proc:
            return None
        yield case

# -------------------------------------------------
# Start defining properties for individual problems
# -------------------------------------------------

def _build_problem_props(*props):
    return {prop.name : prop for prop in props}

ProblemRegistry = _build_problem_props(
    ProblemProps(
        name="sound",
        origin_loc=OriginLoc.LEFT,
        scale_rule=ScaleRule.X_AXIS,
    ),
    ProblemProps(
        name="slow_magnetosonic",
        origin_loc=OriginLoc.LEFT,
        scale_rule=ScaleRule.X_AXIS,
    ),
    ProblemProps(
        name="adiabatic_disk",
        origin_loc=OriginLoc.CENTER,
        scale_rule=ScaleRule.Z_AXIS,
    )
)

# ------------------------------
# 

def _compile_cholla(cholla_dir: str, hostname: str, maketype: str,
                    make_jobs: Union[bool, int]=False):
    main_build_args = ["make"]
    if isinstance(make_jobs, bool) and make_jobs:
        main_build_args.append("-j")
    elif isinstance(make_jobs, bool):
        pass # do nothing
    elif isinstance(make_jobs, int) and make_jobs <= 0:
        raise ValueError("make_jobs can't be 0 or a negative integer")
    elif isinstance(make_jobs, int):
        main_build_args.append("-j")
        main_build_args.append(str(make_jobs))
    else:
        raise TypeError("make_jobs must be a bool or an int")
    main_build_args.append(f"TYPE={maketype}")
    main_build_args.append(f"MACHINE={hostname}")
    commands = [("make", "clean"), main_build_args]

    print(f"About to compile Cholla with TYPE={maketype}")
    for command in commands:
        pretty_subprocess_run(command, cwd=cholla_dir)

def _setup_cholla_for_problem(cholla_dir: str, problem_props: ProblemProps,
                              hostname: str, make_jobs: Union[bool, int]):
    src, _ = problem_props.maketype_and_param_paths()
    dst = os.path.join(cholla_dir, "builds", os.path.basename(src))
    if os.path.exists(dst):
        assert os.path.islink(dst)
        os.unlink(dst)
    os.symlink(src=src, dst=dst, target_is_directory=False)
    maketype = os.path.basename(src)[len(_MAKETYPE_PREFIX):]

    _compile_cholla(
        cholla_dir=cholla_dir, hostname=hostname, maketype=maketype,
        make_jobs=make_jobs
    )

    compiled_cholla_name = f'cholla.{maketype}.{hostname}'
    return compiled_cholla_name

def setup_problem_dir(new_dir: str, cholla_dir: str,
                      problem_props: ProblemProps,
                      problem_cases: list,
                      hostname: str,
                      make_jobs: Union[bool,int] = False):
    os.symlink(
        src=cholla_dir,
        dst=os.path.join(new_dir, "cholla"),
        target_is_directory=True
    )
    _, parfile_path = problem_props.maketype_and_param_paths()
    parfile = os.path.basename(parfile_path)
    os.symlink(src=parfile_path, dst=os.path.join(new_dir, parfile))

    chollabin = _setup_cholla_for_problem(
        cholla_dir, problem_props, hostname, make_jobs=make_jobs
    )
    # todo: copy cholla
    shutil.copy2(src=os.path.join(cholla_dir, "bin", chollabin),
                 dst=os.path.join(new_dir, chollabin))

    # allow us to swap out launcher
    full_launcher_template = "mpirun -np {nproc}"

    field_partemplate_map = {
        'grid_left_xyz' : '{ax}min',
        'grid_width_xyz' : '{ax}len',
        'grid_shape_xyz' : 'n{ax}',
        'nproc_grid_xyz' : 'n_proc_{ax}',
    }

    with open(os.path.join(new_dir, "run_tests.sh"), 'w') as f:
        abs_path = os.path.abspath(new_dir)
        f.write("# move to directory filled with tests!\n")
        f.write(f"cd {abs_path}\n")
        f.write("\n")
        f.write("# run each test\n")
        for problem_case in problem_cases:
            nproc = problem_case.total_proc()
            shell_cmd = [
                full_launcher_template.format(nproc=nproc),
                chollabin,
                parfile
            ]
            for field, parname_template in field_partemplate_map.items():
                for ax, val in zip('xyz', getattr(problem_case, field)):
                    shell_cmd.append(
                        f"{parname_template.format(ax=ax)}={val}"
                    )
            shell_cmd.append('&>')
            shell_cmd.append(f'{problem_props.name}_N{nproc}.log')

            f.write(' '.join(shell_cmd))
            f.write('\n')
        f.write("\n")
        f.write("# done with tests. return to original directory\n")
        f.write("cd -\n")

