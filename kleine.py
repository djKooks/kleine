
import click

@click.group()
def cli():
    pass


@cli.command()
def test():
    '''
    
    '''
    print('test click')



