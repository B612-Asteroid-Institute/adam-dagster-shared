import os
import shutil
import site
import sys
from pathlib import Path

import click

from adam_dagster_shared.patches import sitecustomize


def get_site_packages_dir() -> str:
    """Get the site-packages directory where sitecustomize.py should be installed."""
    # Get all site-packages directories
    site_packages = site.getsitepackages()
    
    # If there's only one, use that
    if len(site_packages) == 1:
        return site_packages[0]
    
    # If there are multiple, try to find one that matches our current environment
    for site_pkg in site_packages:
        if sys.prefix in site_pkg:
            return site_pkg
    
    # If no match found, use the first one
    return site_packages[0]


@click.group()
def cli():
    """CLI commands for adam-dagster-shared package."""
    pass


@cli.command()
def install_sitecustomize():
    """Install sitecustomize.py to the site-packages directory."""
    # Get the source file path
    source_file = Path(sitecustomize.__file__)
    
    # Get the destination directory
    site_packages_dir = get_site_packages_dir()
    dest_file = Path(site_packages_dir) / "sitecustomize.py"
    
    click.echo(f"Installing sitecustomize.py to {dest_file}")
    
    # Create backup if file exists
    if dest_file.exists():
        backup_file = dest_file.with_suffix('.py.bak')
        click.echo(f"Creating backup at {backup_file}")
        shutil.copy2(dest_file, backup_file)
    
    # Copy the file
    shutil.copy2(source_file, dest_file)
    click.echo("Installation complete!")


if __name__ == '__main__':
    cli() 