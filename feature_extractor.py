from tensorflow.keras.preprocessing import image
from tensorflow.keras.applications import Xception
from tensorflow.keras.applications.xception import preprocess_input
import numpy as np
from PIL import Image
import tensorflow as tf
from tensorflow.python.keras.backend import set_session


config = tf.compat.v1.ConfigProto()
config.gpu_options.allow_growth=True
sess = tf.compat.v1.Session(config=config)
graph = tf.compat.v1.get_default_graph()
set_session(sess)

# See https://keras.io/api/applications/ for details


class FeatureExtractor:
    def __init__(self):
        with graph.as_default():
            self.model = Xception(
                        weights="imagenet",
                        classes=1000,
                        classifier_activation="softmax",
                        )

    def extract(self, img):
        """
        Extract a deep feature from an input image
        Args:
            img: from PIL.Image.open(path) or tensorflow.keras.preprocessing.image.load_img(path)
        Returns:
            feature (np.ndarray): deep feature with the shape=(4096, )
        """
        img = Image.open(img)
        img = img.resize((299, 299))  # Xception must take a 299x299 img as an input
        img = img.convert('RGB')  # Make sure img is color
        x = image.img_to_array(img)  # To np.array. Height x Width x Channel. dtype=float32
        x = np.expand_dims(x, axis=0)  # (H, W, C)->(1, H, W, C), where the first elem is the number of img
        x = preprocess_input(x)  # Subtracting avg values for each pixel
        with graph.as_default():
            set_session(sess)
            feature = self.model.predict(x)[0]  # (1, 4096) -> (4096, )
        return feature / np.linalg.norm(feature)  # Normalize
