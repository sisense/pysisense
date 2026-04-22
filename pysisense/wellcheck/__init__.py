from typing import Any

from ..access_management import AccessManagement
from ..dashboard import Dashboard
from ..datamodel import DataModel
from ..sisenseclient import SisenseClient
from .dashboard_checks import DashboardChecksMixin
from .datamodel_checks import DatamodelChecksMixin


class WellCheck(DashboardChecksMixin, DatamodelChecksMixin):
    """
    Collection of wellcheck / health-check methods over Sisense assets.
    WellCheck methods typically analyze Sisense assets (dashboards,
    widgets, data models) for complexity, best practices, and potential issues.
    """

    def __init__(self, api_client: SisenseClient | None = None, debug: bool = False) -> None:
        """
        Initialize the WellCheck helper.

        Parameters
        ----------
        api_client : SisenseClient, optional
            Existing SisenseClient instance. If None, a new SisenseClient will
            be created using the provided debug flag.
        debug : bool, optional
            Enables debug logging when creating an internal SisenseClient.
        """
        if api_client is not None:
            self.api_client = api_client
        else:
            self.api_client = SisenseClient(debug=debug)

        # Use the SisenseClient's logger
        self.logger = self.api_client.logger
        self.dashboard = Dashboard(api_client=self.api_client, debug=debug)
        self.datamodel = DataModel(api_client=self.api_client, debug=debug)
        self.access_mgmt = AccessManagement(api_client=self.api_client, debug=debug)

        self.logger.debug("WellCheck class initialized.")

    def run_full_wellcheck(
        self,
        dashboards: str | list[str] | None = None,
        datamodels: str | list[str] | None = None,
        max_pivot_fields: int = 20,
    ) -> dict[str, Any]:
        """
        Run a composite "full" wellcheck across dashboards and data models.

        This method is a convenience wrapper that orchestrates multiple
        dashboard-level and data-model-level checks and returns a structured
        report that groups their results.

        Parameters
        ----------
        dashboards : str or list of str, optional
            One or more dashboard references to analyze. Each reference can be:
              - a Sisense dashboard ID, or
              - a dashboard title (name).
            At runtime this parameter is tolerant of a single string and will
            normalize it to a one-element list.
        datamodels : str or list of str, optional
            One or more data model references to analyze. Each reference can be:
              - a data model ID, or
              - a data model title (name).
            At runtime this parameter is tolerant of a single string and will
            normalize it to a one-element list.
        max_pivot_fields : int, optional
            Threshold used by the pivot-fields check. Any pivot widget with
            more than this number of fields is flagged.

        Returns
        -------
        dict
            A dictionary with two top-level sections:

            - "dashboards": {
                  "structure": [...],
                  "widget_counts": [...],
                  "pivot_widget_fields": [...],
              }

            - "datamodels": {
                  "custom_tables": [...],
                  "island_tables": [...],
                  "rls_datatypes": [...],
                  "import_queries": [...],
                  "m2m_relationships": [...],
                  "unused_columns": [...],
              }

            Each subsection contains the list of rows returned by the
            corresponding check method. If a given set of references is not
            provided or no assets are successfully processed, that subsection
            will be an empty list.
        """
        self.logger.info("Starting full wellcheck run.")

        # ------------------------------------------------------------------ #
        # Normalize dashboard references                                     #
        # ------------------------------------------------------------------ #
        if dashboards is None:
            dashboard_refs: list[str] = []
        elif isinstance(dashboards, str):
            dashboard_refs = [dashboards]
        else:
            dashboard_refs = [ref for ref in dashboards if isinstance(ref, str)]

        if not dashboard_refs:
            self.logger.info("No dashboard references provided. Dashboard-level checks will be skipped.")

        # ------------------------------------------------------------------ #
        # Normalize datamodel references                                     #
        # ------------------------------------------------------------------ #
        if datamodels is None:
            datamodel_refs: list[str] = []
        elif isinstance(datamodels, str):
            datamodel_refs = [datamodels]
        else:
            datamodel_refs = [ref for ref in datamodels if isinstance(ref, str)]

        if not datamodel_refs:
            self.logger.info("No data model references provided. Data model-level checks will be skipped.")

        dashboards_section: dict[str, Any] = {
            "structure": [],
            "widget_counts": [],
            "pivot_widget_fields": [],
        }

        datamodels_section: dict[str, Any] = {
            "custom_tables": [],
            "island_tables": [],
            "rls_datatypes": [],
            "import_queries": [],
            "m2m_relationships": [],
            "unused_columns": [],
        }

        # ------------------------------------------------------------------ #
        # Dashboard-level checks                                             #
        # ------------------------------------------------------------------ #
        if dashboard_refs:
            self.logger.info("Starting dashboard-level checks in run_full_wellcheck.")

            self.logger.info("Starting dashboard structure check.")
            dashboards_section["structure"] = self.check_dashboard_structure(dashboards=dashboard_refs)
            self.logger.info("Completed dashboard structure check.")

            self.logger.info("Starting dashboard widget-count check.")
            dashboards_section["widget_counts"] = self.check_dashboard_widget_counts(dashboards=dashboard_refs)
            self.logger.info("Completed dashboard widget-count check.")

            self.logger.info("Starting pivot widget-fields check.")
            dashboards_section["pivot_widget_fields"] = self.check_pivot_widget_fields(
                dashboards=dashboard_refs,
                max_fields=max_pivot_fields,
            )
            self.logger.info("Completed pivot widget-fields check.")

            self.logger.info("Completed dashboard-level checks in run_full_wellcheck.")

        # ------------------------------------------------------------------ #
        # Data-model-level checks                                            #
        # ------------------------------------------------------------------ #
        if datamodel_refs:
            self.logger.info("Starting data model-level checks in run_full_wellcheck.")

            self.logger.info("Starting custom-tables check.")
            datamodels_section["custom_tables"] = self.check_datamodel_custom_tables(datamodels=datamodel_refs)
            self.logger.info("Completed custom-tables check.")

            self.logger.info("Starting island-tables check.")
            datamodels_section["island_tables"] = self.check_datamodel_island_tables(datamodels=datamodel_refs)
            self.logger.info("Completed island-tables check.")

            self.logger.info("Starting RLS datatype check.")
            datamodels_section["rls_datatypes"] = self.check_datamodel_rls_datatypes(datamodels=datamodel_refs)
            self.logger.info("Completed RLS datatype check.")

            self.logger.info("Starting import-queries check.")
            datamodels_section["import_queries"] = self.check_datamodel_import_queries(datamodels=datamodel_refs)
            self.logger.info("Completed import-queries check.")

            self.logger.info("Starting many-to-many relationships check.")
            datamodels_section["m2m_relationships"] = self.check_datamodel_m2m_relationships(datamodels=datamodel_refs)
            self.logger.info("Completed many-to-many relationships check.")

            # Unused columns – delegated to AccessManagement
            self.logger.info("Starting unused-columns analysis (delegated to AccessManagement).")
            unused_columns: list[dict[str, Any]] = []
            access_mgmt = getattr(self, "access_mgmt", None)

            if access_mgmt is None:
                self.logger.warning("WellCheck.access_mgmt is not configured. Unused-columns analysis will be skipped in run_full_wellcheck.")
            else:
                unused_columns = access_mgmt.get_unused_columns_bulk(datamodels=datamodel_refs)
                self.logger.info(
                    "Completed unused-columns analysis for %d data model reference(s). Total result rows: %d",
                    len(datamodel_refs),
                    len(unused_columns),
                )

            datamodels_section["unused_columns"] = unused_columns

            self.logger.info("Completed data model-level checks in run_full_wellcheck.")

        self.logger.info("Full wellcheck run completed.")
        return {
            "dashboards": dashboards_section,
            "datamodels": datamodels_section,
        }
