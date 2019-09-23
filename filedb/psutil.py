import errno
import os

if os.name == 'nt':
    import psutil

    pid_exists = psutil.pid_exists
    pid_create_time = lambda pid: psutil.Process(pid).create_time()

else:
    """This is all copy pasted from psutil. Currently it requires OS level dependency when
    installing on Linux, so in order to avoid it, we only rely on psutil on windows as a 
    dependency, and copy paste what is needed otherwise."""


    def pid_exists(pid):
        if pid == 0:
            return True
        try:
            os.kill(pid, 0)
        except OSError as err:
            if err.errno == errno.ESRCH:
                return False
            elif err.errno == errno.EPERM:
                return True
            else:
                raise err
        else:
            return True


    _procfs_path = '/proc'
    CLOCK_TICKS = os.sysconf("SC_CLK_TCK")


    def boot_time():
        """Return the system boot time expressed in seconds since the epoch."""
        path = '%s/stat' % _procfs_path
        with open(path, 'rb') as f:
            for line in f:
                if line.startswith(b'btime'):
                    return float(line.strip().split()[1])
            raise RuntimeError("line 'btime' not found in %s" % path)


    BOOT_TIME = boot_time()


    def _ctime(pid):
        with open("%s/%s/stat" % (_procfs_path, pid), 'rb') as f:
            data = f.read()
        rpar = data.rfind(b')')
        fields = data[rpar + 2:].split()
        return float(fields[19])


    def pid_create_time(pid):
        ctime = _ctime(pid)
        return (ctime / CLOCK_TICKS) + BOOT_TIME
