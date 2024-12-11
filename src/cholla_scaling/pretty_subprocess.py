import asyncio
import shutil
import textwrap

# based on this example
# https://docs.python.org/3.9/library/asyncio-subprocess.html#examples

async def _pretty_subprocess_run(args, **kwargs):

    print("\nexecuting:")
    print(" -> command:", *args)
    if 'cwd' in kwargs:
        print(" -> from:", kwargs["cwd"])

    ncol,_ = shutil.get_terminal_size()
    indent = '    '
    chunk_size = ncol-len(indent)

    def _fmt_output(line):
        line = line.decode('ascii')
        nchunks, remainder = divmod(len(line), chunk_size)
        next_start = 0
        for i in range(0, nchunks + (remainder > 0)):
            cur_start = next_start
            next_start = chunk_size*(i+1)
            print(indent, line[cur_start:next_start], sep='')

    # Create the subprocess; redirect the standard output
    # into a pipe.
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, **kwargs
    )
    while True:
        line = await proc.stdout.readline()
        if line == b'':
            break
        elif line.endswith(b'\n'):
            _fmt_output(line[:-1])
        else:
            _fmt_output(line)
            break

    # Wait for the subprocess exit.
    returncode = await proc.wait()
    completed_output = proc.stdout.at_eof()
    assert completed_output
    print(f"returncode: {returncode}")
    return True

def pretty_subprocess_run(args, **kwargs):
    date = asyncio.run(_pretty_subprocess_run(args, **kwargs))
