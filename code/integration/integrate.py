from sqlalchemy import create_engine

from code.integration.stack_ihme import stack_ihme
from code.integration.denormalize_ghdx import denormalize_ghdx

from code.config import username, password, host, port, database, data_dir


if __name__ == '__main__':
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database))
    connection = engine.connect()

    stack_ihme(engine)
    #denormalize_ghdx(engine)

