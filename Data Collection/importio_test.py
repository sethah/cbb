import logging, json
from importio import importio
from importio import latch

if __name__ == '__main__':
    client = importio.importio(user_id="71d5e979-4eb8-4c63-be70-659810b7e230", api_key="YOUR_API_KEY")
    user_id = '71d5e979-4eb8-4c63-be70-659810b7e230'
    api_key = '71d5e979-4eb8-4c63-be70-659810b7e230:+aJoBpVk/Dy/39oH5mE8ctRmb+F51vGgFdBaRZHEYIiHXuKwfKBHivM635hItycdZTs/sCmhkO9mpRmlnvN5EA=='
    client = importio.importio(user_id="71d5e979-4eb8-4c63-be70-659810b7e230", api_key=api_key)
    client.connect()