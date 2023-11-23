from cnodc.cli.commands import main
from cnodc.boot.boot import init_cnodc

if __name__ == "__main__":
    init_cnodc("cli")
    main()
