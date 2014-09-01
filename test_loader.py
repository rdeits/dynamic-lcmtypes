import importlib
import imp
import os
import subprocess
import tempfile
import sys
import re

class TypeNotFoundError(Exception):
    pass

class HistoricalLCMLoader(object):
    """
    A helper class which can be added to a call to addSubscriber in order to allow the subscriber to decode messages which were generated with an older version of the LCM type definitions.
    """
    def __init__(self, package_name, lcmtypes_path, repo_path):
        self.package_name = package_name
        self.lcmtypes_path = lcmtypes_path
        self.repo_path = repo_path
        self.tmpdir = os.path.join(tempfile.gettempdir(), 'temporary_lcmtypes')
        if not os.path.exists(self.tmpdir):
            os.mkdir(self.tmpdir)
        self.type_cache = {}
        self._mru_shas_cache = {}
        self._initialized = False

        self.build_dir = os.path.join(self.tmpdir, 'build')
        if not os.path.exists(self.build_dir):
            os.mkdir(self.build_dir)

        self.source_dir = os.path.join(self.tmpdir, 'source')
        if not os.path.exists(self.source_dir):
            os.mkdir(self.source_dir)

    def buildTypeAtSHA(self, type_name, sha):
        """
        Build the python source files for the given type and revision. We rename the python module from its default (which is just the LCM package name) to [packagename][sha] to prevent namespace conflicts.
        """
        source_files = self.getOrCreateSourceFiles(type_name, sha, recursive=True)
        sha_source_dir = os.path.dirname(source_files[0])
        sha_build_dir = os.path.join(self.build_dir, sha)
        if not os.path.exists(sha_build_dir):
            os.makedirs(sha_build_dir)
        final_pkg_dir = os.path.join(sha_build_dir, self.package_name + str(sha))
        if not os.path.exists(final_pkg_dir):
            os.makedirs(final_pkg_dir)
        subprocess.check_call("lcm-gen --lazy -p --ppath {build:s} {source:s}".format(
                                      build=sha_build_dir,
                                      source=os.path.join(sha_source_dir, '*')),
                              shell=True)
        build_files = [f for f in os.listdir(os.path.join(sha_build_dir, self.package_name))
                       if f.endswith('.py')]
        build_type_names = [t.replace('.py', '') for t in build_files]
        for f in build_files:
            subprocess.check_call(r"perl -ne 's/{pkg:s}(?=\.({type_list:s}[^a-zA-Z0-9_]))/{pkg:s}{sha:s}/g; print;' < {infile:s} > {outfile:s}".format(
                    pkg=self.package_name,
                    type_list = '|'.join(build_type_names),
                    sha=sha,
                    infile=os.path.join(sha_build_dir, self.package_name, f),
                    outfile=os.path.join(final_pkg_dir, f)),
                                      shell=True)

    def getOrCreateBuildFile(self, type_name, sha):
        fname = type_name + '.py'
        target = os.path.join(self.build_dir, sha, self.package_name + str(sha), fname)
        if not os.path.exists(target):
            self.buildTypeAtSHA(type_name, sha)
        return target

    def getOrCreateSourceFiles(self, type_name, sha, recursive=False):
        """
        Find the LCM source files for the given type at the given revision, pulling them out of the git history as needed. Also finds the source files for the children of that type if recursive=True.
        """
        fname = self.package_name + '_' + type_name + '.lcm'
        source_dir = os.path.join(self.source_dir, sha)
        if not os.path.exists(source_dir):
            os.makedirs(source_dir)
        targets = [os.path.join(self.source_dir, sha, fname)]
        if not os.path.exists(targets[0]):
            try:
                subprocess.check_call("git -C {base:s} show {sha:s}:{typepath:s} > {fpath:s}".format(
                                        base=self.repo_path, sha=sha,
                                        typepath=os.path.join(self.lcmtypes_path, fname),
                                        fpath=targets[0]),
                                     shell=True)
            except subprocess.CalledProcessError:
                raise TypeNotFoundError("The target LCMtype cannot be found at this revision")
        if recursive:
            for child in self.getChildTypes(type_name, sha):
                targets.extend(self.getOrCreateSourceFiles(child, sha, recursive=True))
        return targets

    def getChildTypes(self, type_name, sha):
        """
        Find the children of a given type by parsing the output of lcm-gen -d
        """
        source_file = self.getOrCreateSourceFiles(type_name, sha)[0]
        children = []
        debug_data = subprocess.check_output("lcm-gen -d {fpath:s}".format(fpath=source_file),
                                             shell=True)
        debug_lines = debug_data.split('\n')
        for line in debug_lines:
            line = line.lstrip()
            match = re.match(r"{pkg:s}\.(?P<childname>[^\s]+)".format(pkg=self.package_name), line)
            if match:
                child = (match.groupdict()['childname'])
                if child != type_name:
                    children.append(child)
        return children

    def getSHAsForType(self, type_name):
        """
        Find the git SHAs for all revisions to a particular type
        """
        relative_type_path = os.path.join(self.lcmtypes_path, "{package_name:s}_{type_name:s}.lcm".format(
            package_name=self.package_name, type_name=type_name))
        cdata = subprocess.check_output("git --no-pager -C {0:s} log --pretty=oneline {1:s}".format(
            self.repo_path, relative_type_path), shell=True)
        shas = [c[:40] for c in cdata.split('\n') if len(c) >= 40]
        return shas

    def getSHAsForTypeAndChildren(self, type_name, processed=None):
        """
        Find the git SHAs for all revisions to a type *and* all of its children
        """
        shas = set([])
        if processed is None:
            processed = set([])

        shas.update(self.getSHAsForType(type_name))
        child_shas = set([])
        for sha in shas:
            try:
                for child in self.getChildTypes(type_name, sha):
                    if (child, sha) not in processed:
                        processed.add((child, sha))
                        child_shas.update(self.getSHAsForTypeAndChildren(child, processed))

            except TypeNotFoundError:
                continue
        shas.update(child_shas)
        return shas

    def getTypeAtSHA(self, type_name, sha):
        """
        Get the python class for a given LCM type at a given revision, building it as necessary
        """
        if not (type_name, sha) in self.type_cache:
            build_file = self.getOrCreateBuildFile(type_name, sha)
            build_dir = os.path.join(self.build_dir, sha)
            path = sys.path[:]
            sys.path.insert(0, build_dir)
            module = imp.load_source(type_name, build_file)
            sys.path = path
            self.type_cache[(type_name, sha)] = module.__dict__[type_name]

        return self.type_cache[(type_name, sha)]

    def decode(self, type_name, msg_data):
        """
        Try to decode an LCM message using its historical definitions. Uses a MRU (most recently used) queue of commit SHAs to try to ensure that repeated calls for messages of the same type are fast
        """
        if not self._initialized:
            print "Warning: Possible out-of-date LCM message received. I will not try to decode the message using older versions of the type definition. This will be slow the first time it happens."
            self._initialized = True
        i = 0
        if not type_name in self._mru_shas_cache:
            self._mru_shas_cache[type_name] = list(self.getSHAsForTypeAndChildren(type_name))
        for i, sha in enumerate(self._mru_shas_cache[type_name]):
            try:
                msg_class = self.getTypeAtSHA(type_name, sha)
            except TypeNotFoundError:
                continue
            try:
                # print "Trying to decode using definition of type from commit {sha:s}".format(sha=sha[:8])
                msg_obj = msg_class.decode(msg_data)
            except ValueError as e:
                continue
            self._mru_shas_cache[type_name].pop(i)
            self._mru_shas_cache[type_name].insert(0, sha)
            return msg_obj
        raise ValueError("Unable to decode message data with any available type definitions.")

if __name__ == "__main__":
    import lcm
    lc = lcm.LCM()
    class Handler(object):
        def __init__(self):
            self.last_msg = None
        def handle(self, ch, msg):
            self.last_msg = msg
            print "got one"

    h = Handler()
    lc.subscribe('FOOTSTEP_PLAN_RESPONSE', h.handle)
    lc.handle()

    package_name = 'drc'
    type_name = 'footstep_plan_t'
    lcmtypes_path = "software/drc_lcmtypes/lcmtypes"
    repo_path = os.getenv("DRC_BASE")
    l = HistoricalLCMLoader(package_name, lcmtypes_path, repo_path)

    m = l.decode(type_name, h.last_msg)
    print m

