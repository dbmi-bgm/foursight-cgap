# lambda limit
lambda_limit = 800

# Step Settings
mapper = {'human': 'GRCh38',
          'mouse': 'GRCm38',
          'fruit-fly': 'dm6',
          'chicken': 'galGal5'}

pairs_mapper = {"GRCh38": "hg38",
                "GRCm38": "mm10",
                "dm6": 'dm6',
                "galGal5": "galGal5"}

wf_dict = [
    {
        'app_name': 'md5',
        'workflow_uuid': 'c77a117b-9a58-477e-aaa5-291a109a99f6',
        "config": {
            "ebs_size": 10,
            "instance_type": 't3.small',
            'EBS_optimized': True
        }
    }
]


def step_settings(step_name, my_organism, attribution, overwrite=None):
    """Return a setting dict for given step, and modify variables in
    output files; genome assembly, file_type, desc
    overwrite is a dictionary, if given will overwrite keys in resulting template
    overwrite = {'config': {"a": "b"},
                 'parameters': {'c': "d"},
                 'custom_pf_fields': { 'file_arg': {'e': 'f'}}
                    }
    """
    genome = ""
    genome = mapper.get(my_organism)

    templates = [i for i in wf_dict if i['app_name'] == step_name]
    # every app name should exist only once in wf_dict
    if len(templates) != 1:
        raise ValueError('There are multiple {} settings on wfr_cgap_utils.py'.format(step_name))
    template = templates[0]

    # add genomes to output files
    if template.get('custom_pf_fields'):
        for an_output_file in template['custom_pf_fields']:
            template['custom_pf_fields'][an_output_file]['genome_assembly'] = genome

    update_config = {
        "public_postrun_json": True,
        "behavior_on_capacity_limit": "wait_and_retry"
        }
    if template.get('config'):
        temp_conf = template['config']
        for a_key in update_config:
            if a_key not in temp_conf:
                temp_conf[a_key] = update_config[a_key]
    else:
        template['config'] = update_config

    if not template.get('parameters'):
        template['parameters'] = {}

    template['common_fields'] = attribution
    template['custom_qc_fields'] = {}

    if overwrite:
        for a_key in overwrite:
            for a_spec in overwrite[a_key]:
                # if the key value is a dictionary, use update
                if isinstance(overwrite[a_key][a_spec], dict):
                    template[a_key][a_spec].update(overwrite[a_key][a_spec])
                # if it is string array bool, set the value
                else:
                    template[a_key][a_spec] = overwrite[a_key][a_spec]
    return template
