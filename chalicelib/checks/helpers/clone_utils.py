# 1. case -> get SampleProcessing
# 2. clone samples, patch to individuals
# 3. clone cases, add new Sample + SP items
# case title is a calc prop, should add version - prop in SP item?

pattern = re.compile(r'-v[0-9]+$')


class CaseToClone:

    keep_fields = ['project', 'institution']
    remove_fields = ['uuid', 'submitted_by', 'last_modified', 'schema_version', 'date_created', 'accession']

    def __init__(self, accession, key, new_version):
        self.accession = accession
        self.key = key
        self.new_version = new_version
        self.errors = []
        self.case_metadata = self.get_case_metadata()
        self.old_sample_processing = self.case_metadata.get('sample_processing')
        self.sp_metadata = self.get_sp_metadata()
        self.old_samples = self.sp_metadata.get('samples')
        self.sample_info = self.clone_samples()
        self.patch_individual_samples()
        self.new_sp_item = self.clone_sample_processing()
        self.new_cases = self.clone_cases()

    def append_version_to_value(self, value):
        if value is None:
            return
        if isinstance(value, list):
            return [self.append_version_to_value(item) for item in value if item]
        new_value = re.sub(pattern, '', value)
        return new_value + '-v' + self.new_version

    def get_case_metadata(self):
        return ff_utils.get_metadata(self.accession + '?frame=raw', key=self.key)

    def get_sp_metadata(self):
        if self.old_sample_processing:
            return ff_utils.get_metadata(self.old_sample_processing + '?frame=object', key=self.key)

    def clone_samples(self):
        # need to remove processed_files, completed_processes?
        # remove_fields_sample = []
        results = []
        for sample in self.old_samples:
            try:
                resp = ff_utils.get_metadata(sample + '?frame=raw', key=self.key)
            except Exception as e:
                self.errors.append(e)
            else:
                results.append(resp)
        sample_info = {}
        sample_ids = [result['uuid'] for result in results]
        search_url = f'search/?type=Sample&uuid={"&uuid=".join(sample_ids)}&field=individual&frame=object'
        sample_individual_search = ff_utils.search_metadata(search_url, key=self.key)
        for search_result in sample_individual_search:
            sample_info[search_result['@id']] = {'individual': search_result['individual']['@id']}
        for result in results:
            old_accession = result['accession']
            for field in self.remove_fields:
                if field in result:
                    del result[field]
            # change title? bam_sample_id? etc?
            for field in ['bam_sample_id', 'aliases']:
                if field in result:
                    result[field] = self.append_version_to_value(result[field])
            print(result)
            try:
                post_resp = ff_utils.post_metadata(result, 'sample', key=self.key)
            except Exception as e:
                self.errors.append(e)
            else:
                sample_info[f'/samples/{old_accession}/']['new_id'] = post_resp['@graph'][0]['@id']
        return sample_info

    def patch_individual_samples(self):
        for v in self.sample_info.values():
            try:
                individual_metadata = ff_utils.get_metadata(v['individual'] + '?frame=object', key=self.key)
            except Exception as e:
                self.errors.append(e)
                continue
            try:
                sample_patch = {'samples': individual_metadata.get('samples', []) + [v['new_id']]}
                resp = ff_utils.patch_metadata(sample_patch, v['individual'], key=self.key)
            except Exception as e:
                self.errors.append(e)

    def clone_sample_processing(self):
        keep_fields_sp = ['analysis_type', 'families']
        new_sp_metadata = {}
        for item in self.keep_fields + keep_fields_sp:
            new_sp_metadata[item] = self.sp_metadata[item]
        new_sp_metadata['samples'] = [item['new_id'] for item in self.sample_info.values()]
        # might need to add back some processed files, etc if pipeline is only being rerun at a particular step
        try:
            resp = ff_utils.post_metadata(new_sp_metadata, 'sample_processing', key=self.key)
        except Exception:
            pass
        else:
            return resp['@graph'][0]['@id']

    def clone_cases(self):
        # add report?
        keep_fields_case = [
            'family', 'individual', 'description', 'extra_variant_sample_facets', 'active_filterset', 'case_id'
        ]
        cases = self.sp_metadata.get('cases')
        new_cases = []
        for case in cases:
            try:
                old_case_metadata = ff_utils.get_metadata(case + '?frame=object', key=self.key)
            except Exception as e:
                self.errors.append(e)
            new_case_metadata = {}
            for field in self.keep_fields + keep_fields_case:
                if field in old_case_metadata:
                    new_case_metadata[field] = old_case_metadata.get(field)
            new_case_metadata['sample_processing'] = self.new_sp_item
            try:
                post_resp = ff_utils.post_metadata(new_case_metadata, 'case', key=self.key)
                new_cases.append(post_resp['@graph'][0]['@id'])
            except Exception as e:
                self.errors.append(e)
#             else:
#                 new_cases.append(post_resp['@graph'][0]['@id'])

    # something for meta-workflow-run
