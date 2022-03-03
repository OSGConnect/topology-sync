#!/usr/bin/env python3
import os
import tempfile
import argparse
import logging
import sys
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import List, Set, Tuple
from pathlib import Path

import yaml
from tqdm import tqdm
from git import Repo

from client import UserApiClient


def parse_args(argv: List[str] = sys.argv[1:]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Topology Sync Tool")

    parser.add_argument(
        "github_username",
        type=str,
        help="Github username that will be used to clone topology")

    parser.add_argument("token_file_path",
                        type=str,
                        help="path to file containing Github token")

    parser.add_argument(
        "osgconnect_token_file_path",
        type=str,
        help="path to file containing access token to OSG Connect database")

    return parser.parse_args(argv)


def get_all_osg_projects() -> List[Tuple[str, datetime]]:
    """
    Obtain a list of all projects s.t. project name begins with "root.osg". The output list
    is in the format [("root.osg.example", datetime.datetime(2020, 3, 23, 18, 40, 46, 716576)), ...]
    """
    # get all of the "root.osg" projects
    # NOTE: display_name is not garuanteed to be the same as project name
    date_fmt = "%Y-%b-%d %H:%M:%S.%f %Z"
    osg_projects = list()
    for project in tqdm(
            sorted(
                list(
                    filter(lambda project: "root.osg" in project,
                           client.get_group_list())))):
        project_data = client.get_group(project)["metadata"]

        # don't include the top level project "root.osg" only sub projects
        if "root.osg" != project:
            osg_projects.append(
                (project,
                 datetime.strptime(project_data["creation_date"], date_fmt)))

    return osg_projects


def get_all_projects_added_after_date(
        projects: List[Tuple[str, datetime]],
        date: datetime) -> List[Tuple[str, datetime]]:
    return list(filter(lambda p: p[1] >= date, projects))


def get_topology_files(topology_projects_dir: Path) -> Set[str]:
    return {
        f.name.replace(".yaml", "")
        for f in topology_projects_dir.iterdir() if f.name.endswith(".yaml")
    }


def create_topology_file(project_name: str, dst: Path) -> None:
    # get information about group
    project_info = client.get_group(project_name)["metadata"]

    description = project_info["description"]
    field_of_science = project_info["purpose"]
    organization = client._get(
        "/groups/{}/attributes/OSG:PI_Organization".format(project_name))
    pi_name = client._get(
        "/groups/{}/attributes/OSG:PI_Name".format(project_name))

    # create entry to be written to topology project file
    entry = OrderedDict([("Description", description),
                         ("FieldOfScience", field_of_science),
                         ("Organization", organization), ("PIName", pi_name),
                         ("Sponsor", {
                             "CampusGrid": {
                                 "Name": "OSG Connect"
                             }
                         })])

    # write out new entry
    with dst.open("w") as f:
        yaml.dump(entry, f)


def commit(project: str, topology_repo_path: str) -> None:
    os.chdir(topology_repo_path)
    repo = Repo(".")
    index = repo.index
    origin = repo.remotes.origin  # TODO: need to have a check to ensure we have a remote named origin

    untracked_files = Set(repo.untracked_files)
    project_file_name = "{}.yaml".format(project)
    if project_file_name in untracked_files:
        index.add(project_file_name)
        index.commit("added topology file for new project: {}".format(project))
        origin.push()

        # push to fork
    else:
        # TODO: figure out what to do here, do we want to log an error
        # or do we want to raise an error
        pass


def create_pull_request() -> None:
    pass


if __name__ == "__main__":
    args = parse_args()

    gh_username = args.github_username
    gh_token = None

    # read in github token
    try:
        with open(args.token_file_path, "r") as f:
            gh_token = f.read().strip()
    except FileNotFoundError:
        raise RuntimeError("Unable to open github token file: {}".format(
            args.token_file_path))

    # create client that will be used to connect to OSG Connect DB
    client = UserApiClient(
        token_file_path=str(args.osgconnect_token_file_path))

    # get a list of all osg projects
    all_projects = get_all_osg_projects()

    # filter for all projects added within the last 24 hours
    projects_added_in_last_day = get_all_projects_added_after_date(
        projects=all_projects, date=datetime.now() - timedelta(hours=24))

    # create a tmp dir, where we will clone official osg topology repo
    with tempfile.TemporaryDirectory() as tmpdir_name:
        topology_repo_path = Path(tmpdir_name) / "topology"
        Repo.clone_from(remote, tmpdirname)
        repo = Repo(str(topology_repo_path))

        topology_files = get_topology_files(topology_repo_path / "projects")

        for project in projects_added_in_last_day:
            project_name = project[0].replace("root.osg.", "")

            # if topology file doesn't exist, we will create one
            if project_name not in topology_files:
                create_topology_file(
                    project_name, topology_repo_path / "projects" /
                    "{}.yaml".format(project_name))

                commit(project, str(topology_repo_path))

    # TODO: create pull request
