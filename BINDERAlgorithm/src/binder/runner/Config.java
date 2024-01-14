package binder.runner;

import java.io.File;

import binder.utils.CollectionUtils;

public class Config {

	public enum Algorithm {
		SPIDER, BINDER, BINDERpp
	}
	
	public enum Dataset {
		RANDOM, PLISTA, PLISTA_SMALL, PLISTA_LARGE, TPC_H, TPC_H_10, TPC_H_30, TPC_H_50, TPC_H_70, MUSICBRAINZ, MUSICBRAINZ_SMALL, BIOSQLSP, CATH, CENSUS, COMA, 
		EMDE, ENSEMBL, LOD, SCOP, TESMA, WIKIPEDIA, WIKIRANK, PDB, PDB_SHRINK, LUBM, DBPEDIA, FREEBASE, TEST,
		NCVOTER_STATEWIDE, NCVOTER_STATEWIDE_SMALL
	}
	
	public enum Database {
		MYSQL, DB2, POSTGRESQL, FILE
	}
	
	public Config.Algorithm algorithm = Config.Algorithm.BINDER;
	
	public String databaseURL;
	public String userName;
	public String password;
	public String databaseName;
	public String[] tableNames;
	public Config.Database databaseType;
	public int inputRowLimit; // although specifying the row limit in % is more accurate as it uniformly shrinks a dataset, it is still given in #rows, because % would introduce an unfair overhead to SPIDER (you need to count the row numbers of all tables first to perform % on them)
	
	public String inputFolderPath = "M:\\MA\\data" + File.separator;
	public String inputFileEnding = ".csv";
	public char inputFileSeparator = ',';
	public char inputFileQuotechar = '\"';
	public char inputFileEscape = '\\';//'\0';//
	public int inputFileSkipLines = 0;
	public boolean inputFileStrictQuotes = true;
	public boolean inputFileIgnoreLeadingWhiteSpace = true;
	public boolean inputFileHasHeader = true;
	public boolean inputFileSkipDifferingLines = true; // Skip lines that differ from the dataset's schema
	public String inputFileNullString = "";
	
	public String tempFolderPath = "io" + File.separator + "temp";
	public String measurementsFolderPath = "io" + File.separator + "measurements"; // + "BINDER" + File.separator;
	
	public String statisticsFileName = "IND_statistics.txt";
	public String resultFileName = "IND_results.txt";
	
	public boolean writeResults = true;
	
	public boolean cleanTemp = true;
	
	public boolean detectNary = false;

	public Config() {
		this(Config.Algorithm.BINDER, Config.Database.MYSQL, Config.Dataset.RANDOM);
	}
	
	public Config(Config.Algorithm algorithm, Config.Database database, Config.Dataset dataset) {
		this.algorithm = algorithm;
		this.setSource(database, dataset);
	}

	public Config(Config.Algorithm algorithm, Config.Database database, Config.Dataset dataset, int inputTableLimit, int inputRowLimit) {
		this.algorithm = algorithm;
		this.setSource(database, dataset, inputTableLimit, inputRowLimit);
	}

	public void setSource(Config.Database database, Config.Dataset dataset) {
		this.setSource(database, dataset, -1, -1);
	}

	public void setSource(Config.Database database, Config.Dataset dataset, int inputTableLimit, int inputRowLimit) {
		this.inputRowLimit = inputRowLimit;
		this.setDataset(dataset); // Set the dataset first, because the database needs the dataset to build the connection string
		this.setDatabase(database);
		
		if ((inputTableLimit > 0) && (this.tableNames.length > inputTableLimit)) {
			String[] tableNamesToUse = new String[inputTableLimit];
			for (int i = 0; i < inputTableLimit; i++)
				tableNamesToUse[i] = this.tableNames[i];
			this.tableNames = tableNamesToUse;
		}
	}

	private void setDataset(Config.Dataset dataset) {
		switch (dataset) {
			case RANDOM:
				this.databaseName = "iind";
				this.tableNames = new String[] {"random"};
				break;
			case PLISTA:
				this.databaseName = "plista";
				this.tableNames = new String[] {"statistic", "request", "items", "error"};
				break;
			case PLISTA_SMALL:
				this.databaseName = "iind";
				this.tableNames = new String[] {"statistic_09_14_13", "request_09_14_13", "items_09_14_13", "error_09_14_13"};
				break;
			case PLISTA_LARGE:
				this.databaseName = "iind";
				this.tableNames = new String[] {
						"statistic_1", "statistic_2", "statistic_3", "statistic_4", "statistic_5", 
						"request_1", "request_2", "request_3", "request_4", "request_5", 
						"items_1", "items_2", "items_3", "items_4", "items_5", 
						"error_1", "error_2", "error_3", "error_4", "error_5"};
				break;
			case TPC_H:
				this.databaseName = "tpc_h";
				this.tableNames = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
				break;
			case TPC_H_10:
				this.databaseName = "tpc_h_10";
				this.tableNames = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
				break;
			case TPC_H_30:
				this.databaseName = "tpc_h_30";
				this.tableNames = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
				break;
			case TPC_H_50:
				this.databaseName = "tpc_h_50";
				this.tableNames = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
				break;
			case TPC_H_70:
				this.databaseName = "tpc_h_70";
				this.tableNames = new String[] {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"};
				break;
			case MUSICBRAINZ:
				this.databaseName = "musicbrainz";
				this.tableNames = new String[] {
						"annotation","area","area_alias","area_alias_type","area_annotation","area_gid_redirect","area_tag","area_type","artist","artist_alias","artist_alias_type",
						"artist_annotation","artist_credit","artist_credit_name","artist_gid_redirect","artist_ipi","artist_isni","artist_meta","artist_tag","artist_type","cdtoc",
						"cdtoc_raw","country_area","cover_art_archive.art_type","cover_art_archive.cover_art","cover_art_archive.cover_art_type","cover_art_archive.image_type",
						"cover_art_archive.release_group_cover_art","documentation.l_area_area_example","documentation.l_area_artist_example","documentation.l_area_event_example",
						"documentation.l_area_instrument_example","documentation.l_area_label_example","documentation.l_area_place_example","documentation.l_area_recording_example",
						"documentation.l_area_release_example","documentation.l_area_release_group_example","documentation.l_area_series_example","documentation.l_area_url_example",
						"documentation.l_area_work_example","documentation.l_artist_artist_example","documentation.l_artist_event_example","documentation.l_artist_instrument_example",
						"documentation.l_artist_label_example","documentation.l_artist_place_example","documentation.l_artist_recording_example","documentation.l_artist_release_example",
						"documentation.l_artist_release_group_example","documentation.l_artist_series_example","documentation.l_artist_url_example","documentation.l_artist_work_example",
						"documentation.l_event_event_example","documentation.l_event_instrument_example","documentation.l_event_label_example","documentation.l_event_place_example",
						"documentation.l_event_recording_example","documentation.l_event_release_example","documentation.l_event_release_group_example","documentation.l_event_series_example",
						"documentation.l_event_url_example","documentation.l_event_work_example","documentation.l_instrument_instrument_example","documentation.l_instrument_label_example",
						"documentation.l_instrument_place_example","documentation.l_instrument_recording_example","documentation.l_instrument_release_example",
						"documentation.l_instrument_release_group_example","documentation.l_instrument_series_example","documentation.l_instrument_url_example",
						"documentation.l_instrument_work_example","documentation.l_label_label_example","documentation.l_label_place_example","documentation.l_label_recording_example",
						"documentation.l_label_release_example","documentation.l_label_release_group_example","documentation.l_label_series_example","documentation.l_label_url_example",
						"documentation.l_label_work_example","documentation.l_place_place_example","documentation.l_place_recording_example","documentation.l_place_release_example",
						"documentation.l_place_release_group_example","documentation.l_place_series_example","documentation.l_place_url_example","documentation.l_place_work_example",
						"documentation.l_recording_recording_example","documentation.l_recording_release_example","documentation.l_recording_release_group_example",
						"documentation.l_recording_series_example","documentation.l_recording_url_example","documentation.l_recording_work_example",
						"documentation.l_release_group_release_group_example","documentation.l_release_group_series_example","documentation.l_release_group_url_example",
						"documentation.l_release_group_work_example","documentation.l_release_release_example","documentation.l_release_release_group_example",
						"documentation.l_release_series_example","documentation.l_release_url_example","documentation.l_release_work_example","documentation.l_series_series_example",
						"documentation.l_series_url_example","documentation.l_series_work_example","documentation.l_url_url_example","documentation.l_url_work_example",
						"documentation.l_work_work_example","documentation.link_type_documentation","edit","edit_area","edit_artist","edit_event","edit_instrument","edit_label","edit_note",
						"edit_place","edit_recording","edit_release","edit_release_group","edit_series","edit_url","edit_work","editor_collection_type","editor_sanitised","event",
						"event_alias","event_alias_type","event_annotation","event_gid_redirect","event_meta","event_tag","event_type","gender","instrument","instrument_alias",
						"instrument_alias_type","instrument_annotation","instrument_gid_redirect","instrument_tag","instrument_type","iso_3166_1","iso_3166_2","iso_3166_3","isrc","iswc",
						"l_area_area","l_area_artist","l_area_event","l_area_instrument","l_area_label","l_area_place","l_area_recording","l_area_release","l_area_release_group",
						"l_area_series","l_area_url","l_area_work","l_artist_artist","l_artist_event","l_artist_instrument","l_artist_label","l_artist_place","l_artist_recording",
						"l_artist_release","l_artist_release_group","l_artist_series","l_artist_url","l_artist_work","l_event_event","l_event_instrument","l_event_label","l_event_place",
						"l_event_recording","l_event_release","l_event_release_group","l_event_series","l_event_url","l_event_work","l_instrument_instrument","l_instrument_label",
						"l_instrument_place","l_instrument_recording","l_instrument_release","l_instrument_release_group","l_instrument_series","l_instrument_url","l_instrument_work",
						"l_label_label","l_label_place","l_label_recording","l_label_release","l_label_release_group","l_label_series","l_label_url","l_label_work","l_place_place",
						"l_place_recording","l_place_release","l_place_release_group","l_place_series","l_place_url","l_place_work","l_recording_recording","l_recording_release",
						"l_recording_release_group","l_recording_series","l_recording_url","l_recording_work","l_release_group_release_group","l_release_group_series","l_release_group_url",
						"l_release_group_work","l_release_release","l_release_release_group","l_release_series","l_release_url","l_release_work","l_series_series","l_series_url",
						"l_series_work","l_url_url","l_url_work","l_work_work","label","label_alias","label_alias_type","label_annotation","label_gid_redirect","label_ipi","label_isni",
						"label_meta","label_tag","label_type","language","link","link_attribute","link_attribute_credit","link_attribute_text_value","link_attribute_type",
						"link_creditable_attribute_type","link_text_attribute_type","link_type","link_type_attribute_type","medium","medium_cdtoc","medium_format","medium_index",
						"orderable_link_type","place","place_alias","place_alias_type","place_annotation","place_gid_redirect","place_tag","place_type","recording","recording_alias",
						"recording_alias_type","recording_annotation","recording_gid_redirect","recording_meta","recording_tag","release","release_alias","release_alias_type",
						"release_annotation","release_country","release_gid_redirect","release_group","release_group_alias","release_group_alias_type","release_group_annotation",
						"release_group_gid_redirect","release_group_meta","release_group_primary_type","release_group_secondary_type","release_group_secondary_type_join","release_group_tag",
						"release_label","release_meta","release_packaging","release_raw","release_status","release_tag","release_unknown_country","replication_control","script","series",
						"series_alias","series_alias_type","series_annotation","series_gid_redirect","series_ordering_type","series_tag","series_type","sitemaps.artist_lastmod",
						"sitemaps.control","sitemaps.label_lastmod","sitemaps.place_lastmod","sitemaps.recording_lastmod","sitemaps.release_group_lastmod","sitemaps.release_lastmod",
						"sitemaps.work_lastmod","statistics.statistic","statistics.statistic_event","tag","tag_relation","track","track_gid_redirect","track_raw","url","url_gid_redirect",
						"vote","wikidocs.wikidocs_index","work","work_alias","work_alias_type","work_annotation","work_attribute","work_attribute_type","work_attribute_type_allowed_value",
						"work_gid_redirect","work_meta","work_tag","work_type"};
				this.inputFolderPath = ".." + File.separator + "MusicBrainzProfiler" + File.separator + "data" + File.separator;
				this.inputFileEnding = "";
				this.inputFileSeparator = '\t';
				this.inputFileQuotechar = '\0';
				this.inputFileEscape = '\0';
				this.inputFileSkipLines = 0;
				this.inputFileStrictQuotes = false;
				this.inputFileIgnoreLeadingWhiteSpace = false;
				this.inputFileHasHeader = false;
				this.inputFileSkipDifferingLines = false;
				this.inputFileNullString = "\\N";
				break;
			case MUSICBRAINZ_SMALL:
				this.databaseName = "musicbrainz";
				this.tableNames = new String[] {"area","link","label","place","series","script","work_type","label_type","artist_type"};
				this.inputFolderPath = ".." + File.separator + "MusicBrainzProfiler" + File.separator + "data" + File.separator;
				this.inputFileEnding = "";
				this.inputFileSeparator = '\t';
				this.inputFileQuotechar = '\0';
				this.inputFileEscape = '\0';
				this.inputFileSkipLines = 0;
				this.inputFileStrictQuotes = false;
				this.inputFileIgnoreLeadingWhiteSpace = false;
				this.inputFileHasHeader = false;
				this.inputFileSkipDifferingLines = false;
				this.inputFileNullString = "\\N";
				break;
			case BIOSQLSP:
				this.databaseName = "BIOSQLSP";
				this.tableNames = new String[] {"SG_BIODATABASE", "SG_BIODATABASE_QUALIFIER_ASSOC", "SG_BIOENTRY", "SG_BIOENTRY_ASSOC", "SG_BIOENTRY_DBXREF_ASSOC", "SG_BIOENTRY_PATH", 
												"SG_BIOENTRY_QUALIFIER_ASSOC", "SG_BIOENTRY_REF_ASSOC", "SG_BIOSEQUENCE", "SG_COMMENT", "SG_DBXREF", "SG_DBXREF_QUALIFIER_ASSOC", "SG_LOCATION", 
												"SG_LOCATION_QUALIFIER_ASSOC", "SG_ONTOLOGY", "SG_REFERENCE", "SG_SEQFEATURE", "SG_SEQFEATURE_ASSOC", "SG_SEQFEATURE_DBXREF_ASSOC", "SG_SEQFEATURE_PATH", 
												"SG_SEQFEATURE_QUALIFIER_ASSOC", "SG_SIMILARITY", "SG_TAXON", "SG_TAXON_NAME", "SG_TERM", "SG_TERM_ASSOC", "SG_TERM_DBXREF_ASSOC", "SG_TERM_PATH", 
												"SG_TERM_SYNONYM"};
				break;
			case CATH:
				this.databaseName = "CATH";
				this.tableNames = new String[] {"CHAIN_LIST", "DOMAIN_LIST", "DOMAIN_SEQS", "NAMES", "TESMAEXP"};
				break;
			case CENSUS:
				this.databaseName = "CENSUS";
				this.tableNames = new String[] {"CENSUS", "CENSUS6"};
				break;
			case COMA:
				this.databaseName = "COMA";
				this.tableNames = new String[] {"LINKS"};
				break;
			case EMDE:
				this.databaseName = "EMDE";
				this.tableNames = new String[] {"EXPLAIN_ARGUMENT", "EXPLAIN_DIAGNOSTIC", "EXPLAIN_DIAGNOSTIC_DATA", "EXPLAIN_INSTANCE", "EXPLAIN_OBJECT", "EXPLAIN_OPERATOR", "EXPLAIN_PREDICATE", 
												"EXPLAIN_STATEMENT", "EXPLAIN_STREAM", "MAPPING", "PAGES"};
				break;
			case ENSEMBL:
				this.databaseName = "ENSEMBL";
				this.tableNames = new String[] {"ALT_ALLELE", "ANALYSIS", "ANALYSIS_DESCRIPTION", "ASSEMBLY", "ASSEMBLY_EXCEPTION", "ATTRIB_TYPE", "COORD_SYSTEM", "DENSITY_FEATURE", "DENSITY_TYPE", 
												"DITAG", "DITAG_FEATURE", "DNA", "DNAC", "DNA_ALIGN_FEATURE", "EXON", "EXON_STABLE_ID", "EXON_TRANSCRIPT", "EXTERNAL_DB", "EXTERNAL_SYNONYM", "GENE", 
												"GENE_ARCHIVE", "GENE_ATTRIB", "GENE_STABLE_ID", "GO_XREF", "IDENTITY_XREF", "INTERPRO", "KARYOTYPE", "MAP", "MAPPING_SESSION", "MARKER", "MARKER_FEATURE", 
												"MARKER_MAP_LOCATION", "MARKER_SYNONYM", "META", "META_COORD", "MISC_ATTRIB", "MISC_FEATURE", "MISC_FEATURE_MISC_SET", "MISC_SET", "OBJECT_XREF", 
												"OLIGO_ARRAY", "OLIGO_FEATURE", "OLIGO_PROBE", "PEPTIDE_ARCHIVE", "PREDICTION_EXON", "PREDICTION_TRANSCRIPT", "PROTEIN_ALIGN_FEATURE", "PROTEIN_FEATURE", 
												"QTL", "QTL_FEATURE", "QTL_SYNONYM", "REGULATORY_FACTOR", "REGULATORY_FACTOR_CODING", "REGULATORY_FEATURE", "REGULATORY_FEATURE_OBJECT", 
												"REGULATORY_SEARCH_REGION", "REPEAT_CONSENSUS", "REPEAT_FEATURE", "SEQ_REGION", "SEQ_REGION_ATTRIB", "SIMPLE_FEATURE", "STABLE_ID_EVENT", 
												"SUPPORTING_FEATURE", "T20070911_142817", "T20070911_142817_EXCEPTION", "TRANSCRIPT", "TRANSCRIPT_ATTRIB", "TRANSCRIPT_STABLE_ID", 
												"TRANSCRIPT_SUPPORTING_FEATURE", "TRANSLATION", "TRANSLATION_ATTRIB", "TRANSLATION_STABLE_ID", "UNCONVENTIONAL_TRANSCRIPT_ASSOCIATION", "UNMAPPED_OBJECT", 
												"UNMAPPED_REASON", "XREF"};
				break;
			case LOD:
				this.databaseName = "LOD";
				this.tableNames = new String[] {"EXPERMENTS_THRESHOLDS", "EXPERMENTS_THRESHOLDS_PLI", "PERSON_DE", "PERSON_EN"};
				break;
			case SCOP:
				this.databaseName = "SCOP";
				this.tableNames = new String[] {"CLASSIFICATION", "COMMENTS", "DESCRIPTION", "HIERARCHIE"};
				break;
			case TESMA:
				this.databaseName = "TESMA";
				this.tableNames = new String[] {"DBPEDIA01", "DBPEDIA02", "E1S1"};
				break;
			case WIKIPEDIA:
				this.databaseName = "WIKIPEDIA";
				this.tableNames = new String[] {"IMAGE", "IMAGELINKS"};
				break;
			case WIKIRANK:
				this.databaseName = "WIKIRANK";
				this.tableNames = new String[] {"CIND", "CIND2", "DEBUG_OUTPUT", "R", "R1", "S", "STATS"};
				break;
			case PDB:
				this.databaseName = "PDB";
				this.tableNames = new String[] {"ATOM", "ATOM_SITE", "ATOM_SITES", "ATOM_SITES_ALT", "ATOM_SITES_ALT_ENS", "ATOM_SITES_ALT_GEN", "ATOM_SITES_FOOTNOTE", "ATOM_SITE_ANISOTROP", "ATOM_TYPE", 
												"CELL", "CELL_MEASUREMENT", "CELL_MEASUREMENT_REFLN", "CHEM_COMP", "CHEM_COMP_ANGLE", "CHEM_COMP_ATOM", "CHEM_COMP_BOND", "CHEM_COMP_CHIR", 
												"CHEM_COMP_CHIR_ATOM", "CHEM_COMP_LINK", "CHEM_COMP_PLANE", "CHEM_COMP_PLANE_ATOM", "CHEM_COMP_TOR", "CHEM_COMP_TOR_VALUE", "CHEM_LINK", "CHEM_LINK_ANGLE", 
												"CHEM_LINK_BOND", "CHEM_LINK_CHIR", "CHEM_LINK_CHIR_ATOM", "CHEM_LINK_PLANE", "CHEM_LINK_PLANE_ATOM", "CHEM_LINK_TOR", "CHEM_LINK_TOR_VALUE", "CITATION", 
												"CITATION_AUTHOR", "CITATION_EDITOR", "COMPUTING", "DATABASE_2", "DATABASE_PDB_CAVEAT", "DATABASE_PDB_MATRIX", "DATABASE_PDB_REMARK", "DATABASE_PDB_REV", 
												"DATABASE_PDB_REV_RECORD", "DATABASE_PDB_TVECT", "DIFFRN", "DIFFRN_ATTENUATOR", "DIFFRN_DETECTOR", "DIFFRN_MEASUREMENT", "DIFFRN_ORIENT_MATRIX", 
												"DIFFRN_ORIENT_REFLN", "DIFFRN_RADIATION", "DIFFRN_RADIATION_WAVELENGTH", "DIFFRN_REFLN", "DIFFRN_REFLNS", "DIFFRN_SCALE_GROUP", "DIFFRN_SOURCE", 
												"DIFFRN_STANDARDS", "DIFFRN_STANDARD_REFLN", "ENTITY", "ENTITY_KEYWORDS", "ENTITY_LINK", "ENTITY_NAME_COM", "ENTITY_NAME_SYS", "ENTITY_POLY", 
												"ENTITY_POLY_SEQ", "ENTITY_SRC_GEN", "ENTITY_SRC_NAT", "ENTRY_LINK", "ENTYTYSRCGEN", "EXPTL", "EXPTL_CRYSTAL", "EXPTL_CRYSTAL_FACE", "EXPTL_CRYSTAL_GROW", 
												"EXPTL_CRYSTAL_GROW_COMP", "GEOM", "GEOM_ANGLE", "GEOM_BOND", "GEOM_CONTACT", "GEOM_HBOND", "GEOM_TORSION", "MMS_CATEGORY", "MMS_ENTRY", 
												"MMS_ENTRY_CATEGORIES", "MMS_ITEM", "MMS_SYSTEM", "PDBX_DATABASE_MESSAGE", "PDBX_DATABASE_PDB_OBS_SPR", "PDBX_DATABASE_PROC", "PDBX_DATABASE_RELATED", 
												"PDBX_DATABASE_REMARK", "PDBX_DATABASE_STATUS", "PDBX_ENTITY_ASSEMBLY", "PDBX_ENTITY_NAME", "PDBX_ENTITY_SRC_SYN", "PDBX_NMR_CONSTRAINTS", 
												"PDBX_NMR_DETAILS", "PDBX_NMR_ENSEMBLE", "PDBX_NMR_ENSEMBLE_RMS", "PDBX_NMR_EXPTL", "PDBX_NMR_EXPTL_SAMPLE", "PDBX_NMR_EXPTL_SAMPLE_CONDITIONS", 
												"PDBX_NMR_FORCE_CONSTANTS", "PDBX_NMR_REFINE", "PDBX_NMR_REPRESENTATIVE", "PDBX_NMR_SAMPLE_DETAILS", "PDBX_NMR_SOFTWARE", "PDBX_NMR_SPECTROMETER", 
												"PDBX_NONPOLY_SCHEME", "PDBX_POLY_SEQ_SCHEME", "PDBX_PRERELEASE_SEQ", "PDBX_REFINE", "PDBX_REFINE_AUX_FILE", "PDBX_REFINE_TLS", "PDBX_REFINE_TLS_GROUP", 
												"PDBX_STRUCT_SHEET_HBOND", "PDBX_XPLOR_FILE", "PHASING", "PHASING_AVERAGING", "PHASING_ISOMORPHOUS", "PHASING_MAD", "PHASING_MAD_CLUST", "PHASING_MAD_EXPT", 
												"PHASING_MAD_RATIO", "PHASING_MAD_SET", "PHASING_MIR", "PHASING_MIR_DER", "PHASING_MIR_DER_REFLN", "PHASING_MIR_DER_SHELL", "PHASING_MIR_DER_SITE", 
												"PHASING_MIR_SHELL", "PHASING_SET", "PHASING_SET_REFLN", "REFINE", "REFINE_ANALYZE", "REFINE_B_ISO", "REFINE_HIST", "REFINE_LS_RESTR", "REFINE_LS_RESTR_NCS", 
												"REFINE_LS_SHELL", "REFINE_OCCUPANCY", "REFLN", "REFLNS", "REFLNS_SCALE", "REFLNS_SHELL", "SOFTWARE", "STRUCT", "STRUCT_ASYM", "STRUCT_BIOL", "STRUCT_BIOL_GEN", 
												"STRUCT_BIOL_KEYWORDS", "STRUCT_BIOL_VIEW", "STRUCT_CONF", "STRUCT_CONF_TYPE", "STRUCT_CONN", "STRUCT_CONN_TYPE", "STRUCT_KEYWORDS", "STRUCT_MON_DETAILS", 
												"STRUCT_MON_NUCL", "STRUCT_MON_PROT", "STRUCT_MON_PROT_CIS", "STRUCT_NCS_DOM", "STRUCT_NCS_DOM_LIM", "STRUCT_NCS_ENS", "STRUCT_NCS_ENS_GEN", "STRUCT_NCS_OPER", 
												"STRUCT_REF", "STRUCT_REF_SEQ", "STRUCT_REF_SEQ_DIF", "STRUCT_SHEET", "STRUCT_SHEET_HBOND", "STRUCT_SHEET_ORDER", "STRUCT_SHEET_RANGE", "STRUCT_SHEET_TOPOLOGY", 
												"STRUCT_SITE", "STRUCT_SITE_GEN", "STRUCT_SITE_KEYWORDS", "STRUCT_SITE_VIEW", "SYMMETRY", "SYMMETRY_EQUIV"};
				break;
			case PDB_SHRINK:
				this.databaseName = "PDB";
				this.tableNames = new String[] {"ATOM", "CITATION", "CITATION_AUTHOR", "ENTITY", "PDBX_ENTITY_NAME", "ENTITY_KEYWORDS", "STRUCT", "CELL", "CHEM_COMP", "ATOM_SITE_ANISOTROP", 
												"ATOM_SITES", "ATOM_SITES_ALT", "ATOM_SITES_FOOTNOTE", "ATOM_TYPE", 
												"CITATION_EDITOR", "COMPUTING", "DATABASE_2", "DATABASE_PDB_CAVEAT", "DATABASE_PDB_MATRIX", "DATABASE_PDB_REMARK", "DATABASE_PDB_REV", 
												"DATABASE_PDB_REV_RECORD", "DATABASE_PDB_TVECT", "DIFFRN", "DIFFRN_DETECTOR", "DIFFRN_RADIATION", "DIFFRN_RADIATION_WAVELENGTH", "DIFFRN_REFLNS", "DIFFRN_SOURCE", 
												"ENTITY_LINK", "ENTITY_NAME_COM", "ENTITY_NAME_SYS", "ENTITY_POLY", 
												"ENTITY_POLY_SEQ", "ENTITY_SRC_GEN", "ENTITY_SRC_NAT", "ENTYTYSRCGEN", "EXPTL", "EXPTL_CRYSTAL", "EXPTL_CRYSTAL_GROW", 
												"EXPTL_CRYSTAL_GROW_COMP", "MMS_CATEGORY", "MMS_ENTRY", 
												"MMS_ENTRY_CATEGORIES", "MMS_ITEM", "PDBX_DATABASE_PDB_OBS_SPR", "PDBX_DATABASE_RELATED", 
												"PDBX_DATABASE_STATUS", "PDBX_ENTITY_SRC_SYN", "PDBX_NMR_CONSTRAINTS", 
												"PDBX_NMR_DETAILS", "PDBX_NMR_ENSEMBLE", "PDBX_NMR_ENSEMBLE_RMS", "PDBX_NMR_EXPTL", "PDBX_NMR_EXPTL_SAMPLE", "PDBX_NMR_REFINE", "PDBX_NMR_REPRESENTATIVE", 
												"PDBX_NMR_SAMPLE_DETAILS", "PDBX_NMR_SOFTWARE", "PDBX_NMR_SPECTROMETER", 
												"PDBX_NONPOLY_SCHEME", "PDBX_POLY_SEQ_SCHEME", "PDBX_PRERELEASE_SEQ", "PDBX_REFINE", "PDBX_REFINE_TLS", "PDBX_REFINE_TLS_GROUP", 
												"PDBX_STRUCT_SHEET_HBOND", "PDBX_XPLOR_FILE", "REFINE", "REFINE_ANALYZE", "REFINE_B_ISO", "REFINE_HIST", "REFINE_LS_RESTR", "REFINE_LS_RESTR_NCS", 
												"REFINE_LS_SHELL", "REFINE_OCCUPANCY", "REFLN", "REFLNS", "REFLNS_SCALE", "REFLNS_SHELL", "SOFTWARE", "STRUCT_ASYM", "STRUCT_BIOL", "STRUCT_BIOL_GEN", 
												"STRUCT_CONF", "STRUCT_CONF_TYPE", "STRUCT_CONN", "STRUCT_CONN_TYPE", "STRUCT_KEYWORDS", 
												"STRUCT_MON_PROT", "STRUCT_MON_PROT_CIS", "STRUCT_NCS_DOM", "STRUCT_NCS_DOM_LIM", "STRUCT_NCS_OPER", 
												"STRUCT_REF", "STRUCT_REF_SEQ", "STRUCT_REF_SEQ_DIF", "STRUCT_SHEET", "STRUCT_SHEET_ORDER", "STRUCT_SHEET_RANGE", 
												"STRUCT_SITE", "STRUCT_SITE_GEN", "STRUCT_SITE_KEYWORDS", "SYMMETRY"};
		case LUBM:
			this.databaseName = "lubm_small";
			this.tableNames = new String[] {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17"};
			break;
		case DBPEDIA:
			this.databaseName = "dbpedia";
			this.tableNames = new String[1313];
			for (int i = 0; i < 1313; i++)
				this.tableNames[i] = String.valueOf(i);
			break;
		case FREEBASE:
			this.databaseName = "freebase";
			this.tableNames = new String[11117];
			for (int i = 0; i < 11117; i++)
				this.tableNames[i] = String.valueOf(i);
			break;
		case TEST:
			this.databaseName = "test";
			this.tableNames = new String[] {"test0", "test1", "test2", "test3"};
			break;
		case NCVOTER_STATEWIDE:
			this.databaseName = "ncvoter_Statewide";
			this.tableNames = new String[] {"ncvoter_Statewide_1024001r_71c"};
			this.inputFileSeparator = ',';
			this.inputFileHasHeader = true;
			break;
		case NCVOTER_STATEWIDE_SMALL:
			this.databaseName = "Kaggle\\";
			this.tableNames = new String[] {"enrollement_schoolmanagement_2"};
			this.inputFileSeparator = ',';
			this.inputFileHasHeader = true;

			break;
		default:
			break;
		}
	}

	private void setDatabase(Config.Database database) {
		this.databaseType = database;
		switch (database) {
			case MYSQL:
				this.databaseURL = "jdbc:mysql://localhost:3306/" + this.databaseName;
				this.userName = "root";
				this.password = "berglowe88";
				break;
			case DB2:
				//this.databaseURI = "jdbc:db2://172.16.31.89:50000/";
				//this.userName = "thorsten";
				//this.password = "berglowe88";
				this.databaseURL = "jdbc:db2://localhost:60008/aladin:currentSchema=" + this.databaseName + ";";
				//this.databaseURL = "jdbc:db2://keket:60008/aladin:currentSchema=" + this.databaseName + ";";
				this.userName = "aladin";
				this.password = "oV9Shayo";
				break;
			case POSTGRESQL:
				this.databaseURL = "jdbc:postgresql://localhost:5432/" + this.databaseName;
				this.userName = "thorsten";
				this.password = "berglowe88";
				break;
			case FILE:
				break;
		}
	}

	@Override
	public String toString() {
		return "Config:\r\n\t" +
			"databaseType: " + this.databaseType.name() + "\r\n\t" +
			"databaseURL: " + this.databaseURL + "\r\n\t" +
			"userName: " + this.userName + "\r\n\t" +
			"password: " + this.password + "\r\n\t" +
			"databaseName: " + this.databaseName + "\r\n\t" +
			"tableNames: " + CollectionUtils.concat(this.tableNames, ",") + "\r\n\t" +
			"tempFolderPath" + this.tempFolderPath + "\r\n\t" +
			"cleanTemp" + this.cleanTemp + "\r\n\t" +
			"detectNary" + this.detectNary;
	}
}
