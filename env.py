import grpc

# import certifi
# from grpc.experimental import httpcli


# with open(certifi.where(), 'rb') as f:
#     certificate = f.read()

# credentials = grpc.ssl_channel_credentials(root_certificates=certificate)
# http_channel = httpcli.init_channel('fintekkers-lb-2050451336.us-east-1.elb.amazonaws.com:8082')
# channel = grpc.intercept_channel(http_channel, httpcli.http2_to_http)

LOCAL_CHANNEL = grpc.insecure_channel("127.0.0.1:8082")

# Prod channel points towards the public facing API endpoints. Port 8082 bypasses the broker
# TODO: Reintroduce the broker
PROD_CHANNEL = grpc.secure_channel(
    "api.fintekkers.org:8082", grpc.ssl_channel_credentials()
)
CHANNEL = PROD_CHANNEL
