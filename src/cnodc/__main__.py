from cnodc.system.cli import main
from cnodc.system.boot import init_cnodc

if __name__ == "__main__":
    init_cnodc("cli")
    main()
