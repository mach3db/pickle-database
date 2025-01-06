import pickle
import os
from cryptography.fernet import Fernet
from typing import Optional

class Mach3DBPickleDatabase(object):
    """
        Stores pickled objects in a directory.
        Used for mach3db's remote ramdisk (based on NBD).
        Supports Fernet key for encryption.
    """

    def __init__(self, directory_to_use: str, fernet_key: Optional[str]):
        """

        Initializes the database.
        Requires the directory to use to store data.

        Optionally, a fernet key can be provided to encrypt the data before it is stored.
        Encryption is extremely important if using a remote ramdisk since NBD has no encryption by default.

        :param directory_to_use:
        :param fernet_key:
        """

        self.directory_to_use = directory_to_use

        if fernet_key:
            self.fernet = Fernet(fernet_key)
        else:
            self.fernet = None

    def store_object(self, key: str, value: object):
        """
        Saves the object with pickle to the filepath specified by the key.
        If there is an existing object stored, it will be overwritten.
        Encrypts the pickled object before saving it if a fernet key was provided to the constructor.

        :param key:
        :param value:
        :return:
        """

        path = os.path.join(self.directory_to_use, key)

        fernet = self.fernet

        if fernet:
            with open(path, "wb") as f:
                f.write(
                    fernet.encrypt(
                        pickle.dumps(
                            value,
                            protocol=pickle.HIGHEST_PROTOCOL
                        ),
                    )
                )

                f.flush()
        else:
            with open(path, "wb") as f:
                pickle.dump(
                    value,
                    f,
                    protocol=pickle.HIGHEST_PROTOCOL
                )

                f.flush()

    def retrieve_object(self, key: str):
        """
        Retrieves the pickled object specified by the key.
        Decrypts the pickled object before loading it if a fernet key was provided to the constructor.

        :param key:
        :return:
        """
        path = os.path.join(self.directory_to_use, key)

        fernet = self.fernet

        if fernet:
            with open(path, "rb") as f:
                return pickle.loads(
                    fernet.decrypt(
                        f.read()
                    )
                )
        else:
            with open(path, "rb") as f:
                return pickle.load(f)

    def delete_object(self, key: str):
        """
        Deletes the pickled object specified by the key.
        :param key:
        :return:
        """

        path = os.path.join(self.directory_to_use, key)
        os.remove(path)

def test():
    """
    Tests all functions of Mach3DBPickleDatabase.
    """

    database_directory = '/media/james/0b365b48-8377-4cff-9fa3-79890604d054/user'

    # test with encryption enabled

    fernet_key = Fernet.generate_key().decode()

    db = Mach3DBPickleDatabase(directory_to_use=database_directory, fernet_key=fernet_key)

    test_value = [1, 2, 3] * 100

    db.store_object('test', test_value)

    assert db.retrieve_object('test') == test_value

    db.delete_object('test')

    # test with no encryption enabled

    db = Mach3DBPickleDatabase(directory_to_use=database_directory, fernet_key=None)

    db.store_object('test', test_value)

    assert db.retrieve_object('test') == test_value

    db.delete_object('test')

if __name__ == '__main__':
    test()