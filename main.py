from db_models.mongo_setup import global_init
from db_models.models.cache_model import Cache
from db_models.model.feature_model import Features
import uuid
import globals
import init
from feature_extractor import FeatureExtractor
import pyfiglet
import requests
import pickle


fe = FeatureExtractor()
global_init()


def save_to_db(db_object, feature_to_save, file):
    print("*****************SAVING TO DB******************************")
    feature_obj = Features()
    feature_obj.document = db_object
    feature_obj.feature = feature_to_save
    feature_obj.file = file
    feature_obj.save()
    print("*****************SAVED TO DB******************************")


def update_state(file):
    payload = {
        'topic_name': globals.RECEIVE_TOPIC,
        'client_id': globals.CLIENT_ID,
        'value': file
    }
    requests.request("POST", globals.DASHBOARD_URL,  data=payload)


if __name__ == "__main__":
    print(pyfiglet.figlet_format(str(globals.RECEIVE_TOPIC)))
    print(pyfiglet.figlet_format("INDEXING CONTAINER"))
    print("Connected to Kafka at " + globals.KAFKA_HOSTNAME + ":" + globals.KAFKA_PORT)
    print("Kafka Consumer topic for this Container is " + globals.RECEIVE_TOPIC)
    for message in init.consumer_obj:
        message = message.value
        db_key = str(message)
        db_object = Cache.objects.get(pk=db_key)
        file_name = db_object.file_name
        print("#############################################")
        print("########## PROCESSING FILE " + file_name)
        print("#############################################")
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
                    image_feature = fe.extract(image)
                    feature_to_save = pickle.dumps(image_feature)
                    save_to_db(db_object, feature_to_save, file=images_id)
                print(".....................FINISHED PROCESSING FILE.....................")
                update_state(file_name)

        else:
            """image"""
            with open(file_name, 'wb') as file_to_save:
                file_to_save.write(db_object.file.read())
            image_feature = fe.extract(file_name)
            feature_to_save = pickle.dumps(image_feature)
            file = db_object.file
            save_to_db(db_object, feature_to_save, file)
            print(".....................FINISHED PROCESSING FILE.....................")
            update_state(file_name)
