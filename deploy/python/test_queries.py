#!/usr/bin/env python3
import base64
from abc import ABC, abstractmethod
import io
import json
import logging
from os.path import dirname, join
import subprocess
import sys
import time
from urllib.parse import quote_plus, urlencode
import warnings

import pandas as pd
import pytest
import requests


class RumbleProxy(ABC):
    @abstractmethod
    def run(self, query_file, variables, optimization_config):
        pass


class RumbleCliProxy(RumbleProxy):
    def __init__(self, cmd):
        self.cmd = cmd

    def run(self, query_file, variables, optimization_config):
        # Assemble command
        cmd = [self.cmd]
        for k, v in variables.items():
            cmd += ['--variable:{k}'.format(k=k), v]
        cmd += ['--query-path', query_file]
        optimizations = optimization_config.split(",")
        cmd += ['optimizations-use-type-improvements', 'yes' if optimizations[0] == 'y' else 'no']
        cmd += ['optimizations-use-function-inlining', 'yes' if optimizations[1] == 'y' else 'no']
        cmd += ['optimizations-use-comparison-rewriting', 'yes' if optimizations[2] == 'y' else 'no']
        cmd += ['optimizations-use-projection-pushdown', 'yes' if optimizations[3] == 'y' else 'no']
        cmd += ['optimizations-use-flwor-queries', 'yes' if optimizations[4] == 'y' else 'no']

        # Run query and read result
        output = subprocess.check_output(cmd, encoding='utf-8')

        return [json.loads(line) for line in output.splitlines() if line]


class RumbleServerProxy(RumbleProxy):
    def __init__(self, server_uri):
        self.server_uri = server_uri

    def run(self, query_file, variables, optimization_config):
        args = {'variable:' + quote_plus(k): quote_plus(v)
                for k, v in variables.items()}
        optimizations = optimization_config.split(",")
        args['optimizations-use-type-improvements'] = 'yes' if optimizations[0] == 'y' else 'no'
        args['optimizations-use-function-inlining'] = 'yes' if optimizations[1] == 'y' else 'no'
        args['optimizations-use-comparison-rewriting'] = 'yes' if optimizations[2] == 'y' else 'no'
        args['optimizations-use-projection-pushdown'] = 'yes' if optimizations[3] == 'y' else 'no'
        args['optimizations-use-flwor-queries'] = 'yes' if optimizations[4] == 'y' else 'no'
        args_str = urlencode(args)

        query_uri = '{server_uri}?{args}'.format(
            server_uri=self.server_uri, args=args_str)
        logging.info('Running query against %s', query_uri)
        response = json.loads(requests.post(query_uri, query_file).text)

        if 'warning' in response:
            warning = json.dumps(response['warning'])
            warnings.warn(warning, RuntimeWarning)

        if 'values' in response:
            return response['values']

        if 'error-message' in response:
            raise RuntimeError(response['error-message'])

        raise RuntimeError(str(response))


@pytest.fixture
def rumble(pytestconfig):
    # Use server if provided
    server_uri = pytestconfig.getoption('rumble_server')
    if server_uri:
        logging.info('Using server at %s', server_uri)
        return RumbleServerProxy(server_uri)

    # Fall back to CLI
    rumble_cmd = pytestconfig.getoption('rumble_cmd')
    rumble_cmd = rumble_cmd or join(dirname(__file__), 'rumble.sh')
    logging.info('Using executable %s', rumble_cmd)
    return RumbleCliProxy(rumble_cmd)


def get_file(file):
    req = requests.get(file)
    if req.status_code == requests.codes.ok:
        return req.content
    else:
        print('Content was not found.')
        return None


def test_query(query_id, pytestconfig, rumble):
    num_events = pytestconfig.getoption('num_events')
    num_events = ('-' + str(num_events)) if num_events else ''

    optimizations = pytestconfig.getoption("optimizations")

    query_file = get_file("https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/{}/query.jq".format(query_id))

    if query_file is None:
        raise RuntimeError(str(query_file))
        return

    # Assemble variables
    variables = {}
    input_path = pytestconfig.getoption('input_path')
    # __file__))

    query_file = query_file.replace(b'INPUT_PATH', bytes(input_path, "UTF-8"))

    # Run query and read result
    start_timestamp = time.time()
    output = rumble.run(query_file, variables, optimizations)
    end_timestamp = time.time()
    df = pd.DataFrame.from_records(output)
    print(df)

    running_time = end_timestamp - start_timestamp
    logging.info('Running time: {:.2f}s'.format(running_time))

    # Freeze reference result
    # if pytestconfig.getoption('freeze_result'):
    #     print("HERE in freeze_result", ref_file)
    #     df.to_csv(ref_file, sep=',', index=False)

    # Read reference result
    # df_ref = pd.read_csv(ref_file, sep=',')

    # # Plot histogram
    # if pytestconfig.getoption('plot_histogram'):
    #     from matplotlib import pyplot as plt
    #     plt.hist(df.x, bins=len(df.index), weights=df.y)
    #     plt.savefig(png_file)
    #     plt.close()

    # # Normalize reference and query result
    # df = df[df.y > 0]
    # df = df[['x', 'y']]
    # df.reset_index(drop=True, inplace=True)
    # df_ref = df_ref[df_ref.y > 0]
    # df_ref = df_ref[['x', 'y']]
    # df_ref.reset_index(drop=True, inplace=True)

    # # Assert correct result
    # pd.testing.assert_frame_equal(df_ref, df)


if __name__ == '__main__':
    pytest.main(sys.argv)
