# 1. case -> get SampleProcessing
# 2. clone samples, patch to individuals
# 3. clone cases, add new Sample + SP items
# case title is a calc prop, should add version - prop in SP item?

class CaseToClone:

    keep_fields = ['project', 'institution']
    remove_fields = ['uuid', 'submitted_by', 'last_modified', 'schema_version', 'date_created', 'accession']

    def __init__(self, accession, key, new_version):
        self.accession = accession
        self.key = key
        self.new_version = new_version
        self.case_metadata = self.get_case_metadata()
        self.old_sample_processing = self.metadata.get('sample_processing')
        self.sp_metadata = self.get_sp_metadata()
        self.old_samples = self.sp_metadata.get('samples')
        self.sample_info = self.clone_samples()
        self.patch_individual_samples()
        self.new_sp_item = self.clone_sample_processing()

    def get_case_metadata(self):
        return ff_utils.get_metadata(self.accession + '?frame=raw', key=self.key)

    def get_sp_metadata(self):
        if self.old_sample_processing:
            return ff_utils.get_metadata(self.old_sample_processing + '?frame=object', key=self.key)

    def clone_samples(self):
        results = []
        for item in self.old_samples:
            try:
                resp = ff_utils.get_metadata(sample + '?frame=object', key=self.key)
            except Exception:
                pass
            else:
                results.append(resp)
        sample_info = {}
        for result in results:
            sample_info[result['@id']] = {}
            sample_info[result['@id']]['individual'] = result.get('individual')
            for field in remove_fields:
                if field in result:
                    del result[field]
            # change title? bam_sample_id? etc?
            try:
                post_resp = ff_utils.post_metadata(result, 'sample', key=self.key)
            except Exception:
                pass
            else:
                sample_info[result['@id']]['new_id'] = post_resp['@id']
        return sample_info

    def patch_individual_samples(self):
        for v in self.sample_info.items():
            try:
                individual_metadata = ff_utils.get_metadata(v['individual'], key=self.key)
            except Exception:
                continue
            try:
                sample_patch = {'samples': individual_metadata.get('samples', []) + [v['new_id']]}
                resp = ff_utils.patch_metadata(sample_patch, k, key=self.key)
            except Exception:
                pass

    def clone_sample_processing(self):
        keep_fields_sp = ['analysis_type', 'families']
        new_sp_metadata = {}
        for item in keep_fields + keep_fields_sp:
            new_sp_metadata[item] = self.sp_metadata[item]
        new_sp_metadata['samples'] = [item['new_id'] for item in self.sample_info.values()]
        # might need to add back some processed files, etc if pipeline is only being rerun at a particular step
        try:
            resp = ff_utils.post_metadata(new_sp_metadata, 'sample_processing', key=self.key)
        except Exception:
            pass
        return resp['@id']

    def clone_cases(self):
        keep_fields_case = [
            'family', 'individual', 'description', 'extra_variant_sample_facets', 'active_filterset', 'case_id'
        ]
        cases = self.sp_metadata.get('cases')
        new_cases = []
        for case in cases:
            try:
                old_case_metadata = ff_utils.get_metadata(case, key=self.key)
            except Exception:
                continue
            new_case_metadata = {}
            for field in keep_fields + keep_fields_case:
                new_case_metadata[field] = old_case_metadata.get(field)
            new_case_metadata['sample_processing'] = self.new_sp_item
            try:
                post_resp = ff_utils.post_metadata(new_case_metadata, 'case', key=self.key)
            except Exception:
                continue
            new_cases.append(post_resp['@id'])

    # something for meta-workflow-run
