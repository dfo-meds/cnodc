from pipeman_service.boot import build_processor
if __name__ == "__main__":
    pc = build_processor()
    pc.start()
