from chalicelib.checks.helpers.wfr_utils import *


class TestWfrUtils():

    environ = 'fourfront-cgapwolf'
    my_auth = ff_utils.get_authentication_with_server(ff_env=environ)

    # work in progress
    # def test_init_connection(self):
    #     case_uuid_to_cleanup = 'c551b74d-a6f2-49b3-b919-e1ff174135a0'
    #     res = cleanup(case_uuid_to_cleanup, self.my_auth)
    #     assert res

    def test_order_input_dictionary(self):
        # the function take a dictionary as input
        # keep the keys order but sort the keys value if list or tuple
        # replace value with sorted(value)
        input_dict = {
                   "waldo": "baz",
                   "foobar": ["foo", "bar"],
                   "quux": ('qux', 'fubar'),
                   "bazzes": {"garply", "bars", "corge"}
                   }
        output_dict = {
                   "waldo": "baz",
                   "foobar": ["bar", "foo"],
                   "quux": ['fubar', 'qux'],
                   'bazzes': {'bars', 'corge', 'garply'}
                   }
        assert output_dict == order_input_dictionary(input_dict)

    def test_remove_duplicate_need_runs(self):
        # the function take a list of dictionaries that represents runs
        # compare the run settings and the input files
        # remove duplicate runs with idential settings and input
        # input_files dict is sorted by order_input_dictionary^
        # the function does not sort in any way the run settings
        input_dict_dupl = [
            #an_item
            {
                #an_sp_id
                '/sample-processings/foo/': [
                    #a_run
                    [
                        #run name
                        'foo_run_name',
                        #run settings
                        ['app_name',
                         'organism',
                         {'parameters':
                            {
                            'samples': ['foo-UDN093711', 'foo-UDN289464',
                                        'foo-UDN554872', 'foo-UDN953750'],
                            'waldo': 'baz'
                            }
                          }
                        ],
                        #run input_files
                        {'input_vcf': ['/files-processed/garply/', '/files-processed/corge/'],
                         'additional_file_parameters': {'input_vcf': {'mount': True}}},
                        #all input files
                        'garply_corge'
                    ]
                ]
            },
            {
                '/sample-processings/bar/': [
                    [
                        'bar_run_name',
                        ['app_name',
                         'organism',
                         {'parameters':
                            {
                            'samples': ['foo-UDN093711', 'foo-UDN289464',
                                        'foo-UDN554872', 'foo-UDN953750'],
                            'waldo': 'baz'
                            }
                          }
                        ],
                        {'input_vcf': ['/files-processed/corge/', '/files-processed/garply/'],
                         'additional_file_parameters': {'input_vcf': {'mount': True}}},
                        'corge_garply'
                    ]
                ]
            }
        ]

        input_dict_no_dupl = [
            {
                '/sample-processings/foo/': [
                    [
                        'foo_run_name',
                        ['app_name',
                         'organism',
                         {'parameters':
                            {
                            # samples order is different between the two runs
                            'samples': ['foo-UDN289464', 'foo-UDN093711',
                                        'foo-UDN554872', 'foo-UDN953750'],
                            'waldo': 'baz'
                            }
                          }
                        ],
                        {'input_vcf': ['/files-processed/garply/', '/files-processed/corge/'],
                         'additional_file_parameters': {'input_vcf': {'mount': True}}},
                        'garply_corge'
                    ]
                ]
            },
            {
                '/sample-processings/bar/': [
                    [
                        'bar_run_name',
                        ['app_name',
                         'organism',
                         {'parameters':
                            {
                            'samples': ['foo-UDN093711', 'foo-UDN289464',
                                        'foo-UDN554872', 'foo-UDN953750'],
                            'waldo': 'baz'
                            }
                          }
                        ],
                        {'input_vcf': ['/files-processed/corge/', '/files-processed/garply/'],
                         'additional_file_parameters': {'input_vcf': {'mount': True}}},
                        'corge_garply'
                    ]
                ]
            }
        ]

        output_dict_dupl = [
            {
                '/sample-processings/foo/': [
                    [
                        'foo_run_name',
                        ['app_name',
                         'organism',
                         {'parameters':
                            {
                            'samples': ['foo-UDN093711', 'foo-UDN289464',
                                        'foo-UDN554872', 'foo-UDN953750'],
                            'waldo': 'baz'
                            }
                          }
                        ],
                        {'input_vcf': ['/files-processed/garply/', '/files-processed/corge/'],
                         'additional_file_parameters': {'input_vcf': {'mount': True}}},
                        'garply_corge'
                    ]
                ]
            }
        ]

        assert output_dict_dupl == remove_duplicate_need_runs(input_dict_dupl)
        assert input_dict_no_dupl == remove_duplicate_need_runs(input_dict_no_dupl)
