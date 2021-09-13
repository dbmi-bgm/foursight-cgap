import re
from dcicutils import ff_utils
from magma_ff import create_metawfr


pattern_nospace = re.compile(r'-v[0-9]+$')
pattern_pretty = re.compile(r' \(v[0-9]+\)$')


def try_request(func, *args, **kwargs):
    try:
        resp = func(*args, **kwargs)
    except Exception as e:
        print(e)
    else:
        return resp


class CaseToClone:

    keep_fields = ['project', 'institution']
    remove_fields = ['uuid', 'submitted_by', 'last_modified', 'schema_version', 'date_created', 'accession']

    def __init__(self, accession, key, metawf_uuid, new_version, steps_to_rerun, create_SNV_mwfr=True,
                 keep_SV_mwfr=False, add_bam_to_sample=False, add_gvcf_to_sample=False, add_rck_to_sample=False,
                 add_vep_to_sp=False, add_fullvcf_to_sp=False):
        self.accession = accession
        self.key = key
        self.metawf_uuid = metawf_uuid
        self.new_version = 'v' + str(new_version).lstrip('vV')
        self.steps_to_rerun = steps_to_rerun
        self.create_SNV_mwfr = create_SNV_mwfr
        self.keep_SV_mwfr = keep_SV_mwfr
        self.add_procfiles_to_sample = {
            'bam': add_bam_to_sample,
            'gvcf': add_gvcf_to_sample,
            'rck': add_rck_to_sample
        }
        self.add_procfiles_to_sp = {
            'vep': add_vep_to_sp,
            'full': add_fullvcf_to_sp
        }
        self.errors = []
        self.case_metadata = self.get_case_metadata()
        self.old_sample_processing = self.case_metadata.get('sample_processing')
        self.sp_metadata = self.get_sp_metadata()
        self.old_samples = self.sp_metadata.get('samples')
        self.samples_metadata = self.get_sample_metadata()
        self.sample_info = self.clone_samples()
        self.patch_individual_samples()
        self.new_sp_item = self.clone_sample_processing()
        self.new_case_dict = self.clone_cases()
        self.analysis_type = self.get_analysis_type()
        if self.metawf_uuid and self.analysis_type and self.create_SNV_mwfr:
           self.meta_wfr = self.add_metawfr()

    def append_version_to_value(self, value, pretty=False):
        if value is None:
            return
        if isinstance(value, list):
            return [self.append_version_to_value(item) for item in value if item]
        pattern = pattern_nospace if not pretty else pattern_pretty
        new_value = re.sub(pattern, '', value)
        if pretty:
            return new_value + f' ({self.new_version})'
        return new_value + '-' + self.new_version

    def try_request(self, func, *args, **kwargs):
        try:
            resp = func(*args, **kwargs)
        except Exception as e:
            self.errors.append(e)
        else:
            return resp

    def get_case_metadata(self):
        return ff_utils.get_metadata(self.accession + '?frame=raw', key=self.key)

    def get_sp_metadata(self):
        if self.old_sample_processing:
            return ff_utils.get_metadata(self.old_sample_processing + '?frame=object', key=self.key)

    def get_sample_metadata(self):
        samples_metadata = []
        if self.old_samples:
            for sample in self.old_samples:
                resp = try_request(ff_utils.get_metadata, sample + '?frame=raw', key=self.key)
                if resp:
                    samples_metadata.append(resp)
        return samples_metadata

    def clone_samples(self):
        if not self.samples_metadata:
            return
        sample_info = {}
        sample_ids = [result['uuid'] for result in self.samples_metadata]
        search_url = f'search/?type=Sample&uuid={"&uuid=".join(sample_ids)}&field=individual&field=processed_files'
        sample_individual_search = ff_utils.search_metadata(search_url, key=self.key)
        for search_result in sample_individual_search:
            # find individual which will need to be patched with new sample
            if 'individual' in search_result:
                sample_info[search_result['@id']] = {'individual': search_result['individual']['@id']}
            # find processed files
            if 'processed_files' in search_result:
                sample_info[search_result['@id']]['processed_files'] = []
                for procfile in search_result['processed_files']:
                    sample_info[search_result['@id']]['processed_files'].append(
                        {'@id': procfile['@id'], 'filename': procfile['display_title']}
                    )
        for result in self.samples_metadata:
            sample_id = f'/samples/{result["accession"]}/'
            for field in self.remove_fields:
                if field in result:
                    del result[field]
            # change unique fields
            for field in ['bam_sample_id', 'aliases']:
                if field in result:
                    result[field] = self.append_version_to_value(result[field])
            # add indicated processed files to post json
            result['processed_files'] = []
            for k, v in self.add_procfiles_to_sample.items():
                if v:
                    matching_files = [item['@id'] for item in sample_info[sample_id]['processed_files']
                                     if k in item['display_title']]
                    if matching_files:
                        result['processed_files'].extend(matching_files)
            if not result['processed_files']:
                del result['processed_files']
            post_resp = try_request(ff_utils.post_metadata, result, 'sample', key=self.key)
            if post_resp and sample_id in sample_info:
                sample_info[sample_id]['new_id'] = post_resp['@graph'][0]['@id']
        return sample_info

    def patch_individual_samples(self):
        for v in self.sample_info.values():
            if v.get('individual'):
                individual_metadata = try_request(
                    ff_utils.get_metadata, v['individual'] + '?frame=object', key=self.key
                )
                if not individual_metadata:
                    continue
                sample_patch = {'samples': individual_metadata.get('samples', []) + [v['new_id']]}
                resp = try_request(ff_utils.patch_metadata, sample_patch, v['individual'], key=self.key)

    def clone_sample_processing(self):
        keep_fields_sp = ['analysis_type', 'families']
        new_sp_metadata = {}
        for item in self.keep_fields + keep_fields_sp:
            if item in self.sp_metadata:
                new_sp_metadata[item] = self.sp_metadata.get(item)
        new_sp_metadata['samples'] = [item['new_id'] for item in self.sample_info.values()]
        new_sp_metadata['analysis_version'] = self.new_version

        # add back some processed files, etc if pipeline is only being rerun at a particular step
        if self.sp_metadata.get('processed_files'):
            if self.add_procfiles_to_sp['vep'] or self.add_procfiles_to_sp['full']:
                new_sp_metadata['processed_files'] = []
                for pfile in self.sp_metadata['processed_files']:
                    file_resp = try_request(ff_utils.get_metadata, pfile + '?frame=raw', key=self.key)
                    if file_resp:
                        for key in self.add_procfiles_to_sp:
                            if key in file_resp.get('file_type', '') and self.add_procfiles_to_sp[key]:
                                new_sp_metadata['processed_files'].append(file_resp['@id'])
                                break

        resp = try_request(ff_utils.post_metadata, new_sp_metadata, 'sample_processing', key=self.key)
        if resp:
            return resp['@graph'][0]['@id']

    def clone_cases(self):
        keep_fields_case = [
            'family', 'individual', 'description', 'extra_variant_sample_facets', 'active_filterset', 'case_id'
        ]
        if self.keep_SV_mwfr:
            keep_fields_case.append('meta_workflow_run_sv')
        cases = self.sp_metadata.get('cases')
        new_case_dict = {}
        for case in cases:
            old_case_metadata = try_request(ff_utils.get_metadata, case + '?frame=object', key=self.key)
            if not old_case_metadata:
                continue
            new_case_metadata = {}
            for field in self.keep_fields + keep_fields_case:
                if field in old_case_metadata:
                    new_case_metadata[field] = old_case_metadata.get(field)
            new_case_metadata['sample_processing'] = self.new_sp_item
            if not 'case_id' in new_case_metadata:
                new_case_metadata['case_id'] = old_case_metadata.get('case_title')
            new_case_metadata['case_id'] = self.append_version_to_value(new_case_metadata['case_id'], pretty=True)

            if old_case_metadata.get('report'):
                new_report_json = {
                    'project': old_case_metadata['project'],
                    'institution': old_case_metadata['institution']
                }
                report = try_request(ff_utils.post_metadata, new_report_json, 'report', key=self.key)
                if report:
                    new_case_metadata['report'] = report['@graph'][0]['@id']

            post_resp = try_request(ff_utils.post_metadata, new_case_metadata, 'case', key=self.key)
            if post_resp:
                new_accession = post_resp['@graph'][0]['accession']
                new_case_dict[old_case_metadata['accession']] = {
                    'new case uuid': post_resp['@graph'][0]['uuid'],
                    'new case accession': new_accession
                }
                patch_resp = try_request(ff_utils.patch_metadata, {'superseded_by': new_accession},
                                         old_case_metadata['@id'], key=self.key)
        return new_case_dict

    def get_analysis_type(self):
        # figure out if analysis will be trio or proband only or proband-only cram
        sp_type = self.sp_metadata.get('analysis_type', '')
        if not sp_type or not sp_type.startswith('WGS'):
            return None
        if sp_type.endswith('Trio'):
            return 'trio'
        elif sp_type.endswith('Group'):
            # figure out if trio+ or if parents aren't present
            sample_relations = [item.get('relationship') for item in self.sp_metadata.get('samples_pedigree', [{}])]
            if all(item in sample_relations for item in ['proband', 'mother', 'father']):
                return 'trio'
        # if not yet returned, then it is a proband-only analysis
        if all(sample.get('cram_files') for sample in self.samples_metadata):
            return 'cram proband'
        return 'proband'

    def add_metawfr(self):
        metawfr_json = create_metawfr.create_metawfr_from_case(
            metawf_uuid=self.metawf_uuid,
            case_uuid=self.new_case_dict[self.accession]['new case uuid'],
            type=f'WGS {self.analysis_type}',
            ff_key=self.key,
            post=True,
            patch_case=True,
            verbose=False)
        # keep commented out lines below for future development
        # metawfr_json = import_metawfr.import_metawfr(
        #     metawf_uuid=self.metawf_uuid,
        #     metawfr_uuid=self.case_metadata['meta_workflow_run'],
        #     case_uuid=self.case_metadata['uuid'],
        #     steps_name=self.steps_to_rerun,
        #     create_metawfr=create_metawfr.create_metawfr_from_case,
        #     type=f'WGS {self.analysis_type}',
        #     ff_key=self.key,
        #     post=False,
        #     verbose=False
        # )
        return metawfr_json
