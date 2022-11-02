CNV_dict = {
    'workflow_annotateSV_sansa_vep_vcf-check': {
                         'dependency': 'workflow_manta_vcf-check',
                         'output': 'annotated_SV_vcf',
                         'output_match': 'manta_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_SV_cytoband_vcf-check': {
                         'dependency': 'workflow_20_unrelated_SV_filter_vcf-check',
                         'output': 'cytoband_SV_vcf',
                         'output_match': '20_unrelated_filtered_SV_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_SV_annotation_cleaner_vcf-check': {
                         'dependency': 'workflow_SV_length_filter_vcf-check',
                         'output': 'higlass_SV_vcf',
                         'output_match': 'length_filtered_SV_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         }
    }

proband_SNV_dict = {
    # BAM
    'workflow_add-readgroups-check': {
                        'dependency': 'workflow_bwa-mem_no_unzip-check',
                        'output': 'bam_w_readgroups',
                        'output_match': 'raw_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_merge-bam-check': {
                        'dependency': 'workflow_add-readgroups-check',
                        'output': 'merged_bam',
                        'output_match': 'bam_w_readgroups',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_picard-MarkDuplicates-check':{
                        'dependency': 'workflow_merge-bam-check',
                        'output': 'dupmarked_bam',
                        'output_match': 'merged_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_sort-bam-check': {
                        'dependency': 'workflow_picard-MarkDuplicates-check',
                        'output': 'sorted_bam',
                        'output_match': 'dupmarked_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_gatk-ApplyBQSR-check': {
                        'dependency': 'workflow_sort-bam-check',
                        'output': 'recalibrated_bam',
                        'output_match': 'sorted_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    # VCF
    'workflow_samplegeno': {
                         'dependency': 'workflow_gatk-GenotypeGVCFs-check',
                         'output': 'samplegeno_vcf',
                         'output_match': 'vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    # 'workflow_vep-annot-check': {
    #                      'dependency': 'workflow_samplegeno',
    #                      'output': 'annotated_vcf',
    #                      'output_match': 'samplegeno_vcf',
    #                      'key': 'Total Variants Called',
    #                      'key_match': 'Filtered Variants'
    #                      },
    'workflow_granite-comHet-check': {
                         'dependency': 'workflow_granite-filtering-check',
                         'output': 'comHet_vcf',
                         'output_match': 'merged_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_dbSNP_ID_fixer-check': {
                         'dependency': 'workflow_granite-comHet-check',
                         'output': 'vcf',
                         'output_match': 'comHet_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_hg19lo_hgvsg-check': {
                         'dependency': 'workflow_dbSNP_ID_fixer-check',
                         'output': 'vcf',
                         'output_match': 'vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         }
    }

trio_SNV_dict = {
    # BAM
    'workflow_add-readgroups-check': {
                        'dependency': 'workflow_bwa-mem_no_unzip-check',
                        'output': 'bam_w_readgroups',
                        'output_match': 'raw_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_merge-bam-check': {
                        'dependency': 'workflow_add-readgroups-check',
                        'output': 'merged_bam',
                        'output_match': 'bam_w_readgroups',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_picard-MarkDuplicates-check':{
                        'dependency': 'workflow_merge-bam-check',
                        'output': 'dupmarked_bam',
                        'output_match': 'merged_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_sort-bam-check': {
                        'dependency': 'workflow_picard-MarkDuplicates-check',
                        'output': 'sorted_bam',
                        'output_match': 'dupmarked_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    'workflow_gatk-ApplyBQSR-check': {
                        'dependency': 'workflow_sort-bam-check',
                        'output': 'recalibrated_bam',
                        'output_match': 'sorted_bam',
                        'key': 'Total Reads',
                        'key_match': 'Total Reads'
                        },
    # VCF
    'workflow_samplegeno': {
                         'dependency': 'workflow_gatk-GenotypeGVCFs-check',
                         'output': 'samplegeno_vcf',
                         'output_match': 'vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    # 'workflow_vep-annot-check': {
    #                      'dependency': 'workflow_samplegeno',
    #                      'output': 'annotated_vcf',
    #                      'output_match': 'samplegeno_vcf',
    #                      'key': 'Total Variants Called',
    #                      'key_match': 'Filtered Variants'
    #                      },
    'workflow_granite-novoCaller-rck-check': {
                         'dependency': 'workflow_granite-filtering-check',
                         'output': 'novoCaller_vcf',
                         'output_match': 'merged_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_granite-comHet-check': {
                         'dependency': 'workflow_granite-novoCaller-rck-check',
                         'output': 'comHet_vcf',
                         'output_match': 'novoCaller_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_dbSNP_ID_fixer-check': {
                         'dependency': 'workflow_granite-comHet-check',
                         'output': 'vcf',
                         'output_match': 'comHet_vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         },
    'workflow_hg19lo_hgvsg-check': {
                         'dependency': 'workflow_dbSNP_ID_fixer-check',
                         'output': 'vcf',
                         'output_match': 'vcf',
                         'key': 'Filtered Variants',
                         'key_match': 'Filtered Variants'
                         }
    }
