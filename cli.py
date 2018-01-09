import logging
import click
import scraping
import settings


logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def cli(ctx):
    init_logging(settings.LOG_LEVEL)


@cli.command()
@click.pass_context
def run(ctx):
    """Run scrape"""
    httpc = scraping.GRequestsHttpClient()
    scraper = scraping.Scraper(httpc, settings)
    scraper.run()


@cli.command()
@click.pass_context
def newrun(ctx):
    """Reset database, then run scrape"""
    ctx.invoke(run)


def init_logging(level=logging.INFO):
    """Set up logging parameters"""
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s %(message)s', level=level)

    warn_only = ['urllib3', 'chardet']
    # warn_only = []
    for pkgname in warn_only:
        logging.getLogger(pkgname).setLevel(logging.WARN)


if __name__ == '__main__':
    cli(obj={})
