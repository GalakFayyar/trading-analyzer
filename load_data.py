#!/usr/bin/env python
"""
    Intègres les données à destination de l'application GeoPro dans Elasticsearch

    Usage:
        init_data.py --type_doc=<doc_type> --source_folder=<folder_path> [--debug] 

    Example:
        python init_data.py --type_doc=crypto --source_folder=./data/

    Options:
        --help                              Affiche l'aide
        --type_doc=<doc_type>               ype de document à traiter
        --source_filefolder=<folder_path>     Fichier contenant les données à importer ou à mettre à jour
"""
from elasticsearch import Elasticsearch, TransportError
from logger import logger, configure
from docopt import docopt
import json, time

from swallow.inout.ESio import ESio
from swallow.inout.CSVio import CSVio
from swallow.inout.JsonFileio import JsonFileio
from swallow.Swallow import Swallow

def file_to_elasticsearch(p_docin, p_type, p_es_conn, p_es_index, p_arguments):
    doc = {
        'date_operation': p_docin[0]
    }
    for pair in doc_in[1:]:
        currency = pair.split(':')[0]
        value = pair.split(':')[1]
        doc[currency] = {
            'libelle': currency,
            'value': value
        }

    document = {
        '_op_type': 'update',
        '_type': p_type,
        'script': "ctx._source = doc",
        'params': {
            "doc": doc
        },
        'upsert': doc,
        '_retry_on_conflict': 100
    }

    return [document]

def run_import(type_doc = None, source_file = None):
    conf = json.load(open('./init-conf.json'))

    # Command line args
    arguments = docopt(__doc__, version=conf['version'])

    configure(conf['log']['level_values'][conf['log']['level']],
              conf['log']['dir'], 
              conf['log']['filename'],
              conf['log']['max_filesize'], 
              conf['log']['max_files'])

    #
    #   Création du mapping
    # 

    es_mappings = json.load(open('data/es.mappings.json'))

    # Connexion ES métier
    try:
        param = [{'host': conf['connectors']['elasticsearch']['host'],
                  'port': conf['connectors']['elasticsearch']['port']}]
        es = Elasticsearch(param)
        logger.info('Connected to ES Server: %s', json.dumps(param))
    except Exception as e:
        logger.error('Connection failed to ES Server : %s', json.dumps(param))
        logger.error(e)

    # Création de l'index ES metier cible, s'il n'existe pas déjà
    index = conf['connectors']['elasticsearch']['index']
    if not es.indices.exists(index):
        logger.debug("L'index %s n'existe pas : on le crée", index)
        body_create_settings = {
            "settings" : {
                "index" : {
                    "number_of_shards" : conf['connectors']['elasticsearch']['number_of_shards'],
                    "number_of_replicas" : conf['connectors']['elasticsearch']['number_of_replicas']
                },
                "analysis" : {
                    "analyzer": {
                        "lower_keyword": {
                            "type": "custom",
                            "tokenizer": "keyword",
                            "filter": "lowercase"
                        }
                    }
                }
            }
        }
        es.indices.create(index, body=body_create_settings)
        # On doit attendre 5 secondes afin de s'assurer que l'index est créé avant de poursuivre
        time.sleep(2)

        # Création des type mapping ES
        for type_es, properties in es_mappings['geopro'].items():
            logger.debug("Création du mapping pour le type de doc %s", type_es)
            es.indices.put_mapping(index=index, doc_type=type_es, body=properties)

        time.sleep(2)

    #
    #   Import des données initiales
    #

    # Objet swallow pour la transformation de données
    swal = Swallow()

    # Tentative de récupération des paramètres en argument
    type_doc = arguments['--type_doc'] if not type_doc else type_doc
    source_file = arguments['--source_file'] if not source_file else ('./upload/' + source_file)

    # On lit dans un fichier
    reader = CSVio()
    swal.set_reader(reader, p_file=source_file, p_delimiter='|')

    # On écrit dans ElasticSearch
    writer = ESio(conf['connectors']['elasticsearch']['host'],
                  conf['connectors']['elasticsearch']['port'],
                  conf['connectors']['elasticsearch']['bulk_size'])
    swal.set_writer(writer, p_index=conf['connectors']['elasticsearch']['index'], p_timeout=30)

    # On transforme la donnée avec la fonction
    swal.set_process(file_to_elasticsearch, p_type=type_doc, p_es_conn=es, p_es_index=conf['connectors']['elasticsearch']['index'], p_arguments=arguments)

    logger.debug("Indexation sur %s du type de document %s", conf['connectors']['elasticsearch']['index'], type_doc)
    
    swal.run(1)

    logger.debug("Opération terminée pour le type de document %s ", type_doc)

if __name__ == '__main__':
    run_import()