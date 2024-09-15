def handler(event, context):

    print(event)

    return {
        "status_code": 200,
        "body": event
    }