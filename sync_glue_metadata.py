from etl_manager.meta import read_database_folder
db = read_database_folder('meta_data/')
db.create_glue_database(delete_if_exists=True)

