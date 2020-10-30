from db_models.mongo_setup import global_init
from db_models.models.cache_model import Cache
from db_models.models.feature_model import Features
import uuid
import globals
import init
from feature_extractor import FeatureExtractor
import pyfiglet
import requests
import pickle
from init import err_logger


fe = FeatureExtractor()
global_init()
FILE_ID = ""


def save_to_db(db_object, feature_to_save, file):
    try:
        print("*****************SAVING TO DB******************************")
        print("in save")
        feature_obj = Features()
        feature_obj.document = db_object
        feature_obj.feature = feature_to_save
        feature_obj.file = file
        feature_obj.save()
        print("*****************SAVED TO DB******************************")
    except Exception as e:
        print(" ERROR IN SAVE TO DB")
        err_logger(str(e)+" ERROR IN SAVE TO DB")


def update_state(file_name):
    payload = {
        'parent_name': globals.PARENT_NAME,
        'group_name': globals.GROUP_NAME,
        'container_name': globals.RECEIVE_TOPIC,
        'file_name': file_name,
        'client_id': globals.CLIENT_ID
    }
    try:
        requests.request("POST", globals.DASHBOARD_URL,  data=payload)
    except Exception as e:
        print(f"{e} EXCEPTION IN UPDATE STATE API CALL......")
        err_logger(f"{e} EXCEPTION IN UPDATE STATE API CALL......FILE ID {FILE_ID}")


if __name__ == "__main__":
    print(pyfiglet.figlet_format(str(globals.RECEIVE_TOPIC)))
    print(pyfiglet.figlet_format("INDEXING CONTAINER"))
    print("Connected to Kafka at " + globals.KAFKA_HOSTNAME + ":" + globals.KAFKA_PORT)
    print("Kafka Consumer topic for this Container is " + globals.RECEIVE_TOPIC)
    for message in init.consumer_obj:
        message = message.value
        db_key = str(message)
        print(db_key, 'db_key')
        FILE_ID = db_key
        try:
            db_object = Cache.objects.get(pk=db_key)
            file_name = db_object.file_name
            print("#############################################")
            print("########## PROCESSING FILE " + file_name)
            print("#############################################")
        except Exception as e:
            print("EXCEPTION IN FETCHING FROM DATABASE......")
            err_logger(str(e) + " EXCEPTION IN FETCHING FROM DATABASE......FILE ID " + FILE_ID)
            continue
        if db_object.is_doc_type:
            """document"""
            if db_object.contains_images:
                images_array = []
                images_id = []
                for image in db_object.files:
                    pdf_image = str(uuid.uuid4()) + ".jpg"
                    with open(pdf_image, 'wb') as file_to_save:
                        file_to_save.write(image.file.read())
                    images_array.append(pdf_image)
                    images_id.append(image.file)
                to_save = list()
                for image, image_id in zip(images_array, images_id):
                    try:
                        image_feature = fe.extract(image)
                        feature_to_save = pickle.dumps(image_feature)
                        save_to_db(db_object, feature_to_save, file=image_id)
                    except Exception as e:
                        print(str(e) + "Exception in predict")
                        err_logger(str(e) + "Exception in predict")
                        continue
                print(".....................FINISHED PROCESSING FILE.....................")
                update_state(file_name)
            else:
                print(".....................FINISHED PROCESSING FILE.....................")

        else:
            """image"""
            with open(file_name, 'wb') as file_to_save:
                file_to_save.write(db_object.file.read())
            try:
                image_feature = fe.extract(file_name)
                print("Features Extracted")
                feature_to_save = pickle.dumps(image_feature)
                file = db_object.file
                save_to_db(db_object, feature_to_save, file)
                print(".....................FINISHED PROCESSING FILE.....................")
                update_state(file_name)
            except Exception as e:
                print(str(e) + " Exception in predict")
                err_logger(str(e) + " Exception in predict")
