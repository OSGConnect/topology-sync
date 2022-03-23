#!/usr/bin/env python3
import pprint
import datetime
from pathlib import Path

import yaml
import pytest

import client
import sync

pp = pprint.PrettyPrinter(indent=4)

@pytest.fixture
def fake_client():
    return client.UserApiClient(Path(__file__))

def test_get_all_osg_projects(mocker, fake_client):
    mocker.patch("client.UserApiClient.get_group_list", return_value=["root.osg.TEST-PROJECT"])
    mocker.patch(
            "client.UserApiClient.get_group", 
            return_value={
                'apiVersion': 'v1alpha1',
                'kind': 'Group',
                'metadata': {'name': 'root.osg.TEST-PROJECT',
                'display_name': 'UC-Staff',
                'email': 'test@test.edu',
                'phone': '123-456-7890',
                'purpose': 'Computer Sciences',
                'description': 'this is a test description',
                'creation_date': '2022-Jan-01 01:01:01.000000 UTC',
                'unix_id': 1234,
                'pending': False}}
        )

    assert sync.get_all_osg_projects(fake_client) == [("root.osg.TEST-PROJECT", datetime.datetime(2022, 1, 1, 1, 1, 1))]

def test_get_all_projects_added_after_date():
    projects = [
                ("test1", datetime.datetime(year=2022, month=1, day=1, hour=1, minute=1, second=1)),
                ("test2", datetime.datetime(year=2022, month=1, day=2, hour=1, minute=1, second=1)),
                ("test3", datetime.datetime(year=2022, month=1, day=3, hour=1, minute=1, second=1)),
                ("test4", datetime.datetime(year=2022, month=1, day=4, hour=1, minute=1, second=1)),
            ]

    result = sync.get_all_projects_added_after_date(
                projects=projects, 
                date=datetime.datetime(year=2022, month=1, day=3, hour=1, minute=1, second=1)
            )

    assert result ==  [
                ("test3", datetime.datetime(year=2022, month=1, day=3, hour=1, minute=1, second=1)),
                ("test4", datetime.datetime(year=2022, month=1, day=4, hour=1, minute=1, second=1)),
            ]

def test_get_topology_files(tmpdir):
    test_file = tmpdir / "test.yaml"
    test_file.write("test", mode="w")

    collected_projects = sync.get_topology_files(Path(str(tmpdir)))
    assert collected_projects == {"test"}

def test_create_topology_file(mocker, tmpdir, fake_client):
    mocker.patch("client.UserApiClient.get_group", return_value={"metadata": {"description": "test desc", "purpose": "cs"}})
    mocker.patch("client.UserApiClient._get")
    client.UserApiClient._get.side_effect = ["test-org", "pi-name"]

    dst = Path(str(tmpdir / "test-project.yaml"))
    sync.create_topology_file(client=fake_client, project_name="test-project", dst=dst)

    expected = {
            "Description": "test desc",
            "FieldOfScience": "cs",
            "Organization": "test-org",
            "PIName": "pi-name",
            "Sponsor": {"CampusGrid": {"Name": "OSG Connect"}}
        }

    with dst.open("r") as f:
        result = yaml.load(f, Loader=yaml.Loader)

    assert result == expected
