from os import getenv

from typing import Optional
from agno.tools.linear import LinearTools as AgnoLinearTools
from .common import make_base, wrap_tool


class Linear(make_base(AgnoLinearTools)):
    api_token: str | None = None

    def _get_tool(self):
        # a workaround for Linear tool not accepting the api token as argument,
        # only as environment variable; they have fixed that in agno>=2.0.0,
        # but update requires non-zero effort
        if self.api_token is not None:

            def my_getenv(key, *a, **kw):
                if key == "LINEAR_API_KEY":
                    return self.api_token
                return getenv(key, *a, **kw)

            # monkey-patch the function accessed by LinearTools() init so that
            # it gets the api key correctly
            self.Inner.__init__.__globals__["getenv"] = my_getenv
        try:
            return self.Inner(
                get_user_details=True,
                get_issue_details=True,
                create_issue=True,
                update_issue=True,
                get_user_assigned_issues=True,
                get_workflow_issues=True,
                get_high_priority_issues=True,
            )
        finally:
            # return original function back
            self.Inner.__init__.__globals__["getenv"] = getenv

    @wrap_tool("agno__linear__get_user_details", AgnoLinearTools.get_user_details)
    def get_user_details(self) -> Optional[str]:
        return self._tool.get_user_details()

    @wrap_tool("agno__linear__get_issue_details", AgnoLinearTools.get_issue_details)
    def get_issue_details(self, issue_id: str) -> Optional[str]:
        return self._tool.get_issue_details(issue_id)

    @wrap_tool("agno__linear__create_issue", AgnoLinearTools.create_issue)
    def create_issue(
        self,
        title: str,
        description: str,
        team_id: str,
        project_id: str,
        assignee_id: str,
    ) -> Optional[str]:
        return self._tool.create_issue(
            title, description, team_id, project_id, assignee_id
        )

    @wrap_tool("agno__linear__update_issue", AgnoLinearTools.update_issue)
    def update_issue(self, issue_id: str, title: Optional[str]) -> Optional[str]:
        return self._tool.update_issue(issue_id, title)

    @wrap_tool(
        "agno__linear__get_user_assigned_issues",
        AgnoLinearTools.get_user_assigned_issues,
    )
    def get_user_assigned_issues(self, user_id: str) -> Optional[str]:
        return self._tool.get_user_assigned_issues(user_id)

    @wrap_tool("agno__linear__get_workflow_issues", AgnoLinearTools.get_workflow_issues)
    def get_workflow_issues(self, workflow_id: str) -> Optional[str]:
        return self._tool.get_workflow_issues(workflow_id)

    @wrap_tool(
        "agno__linear__get_high_priority_issues",
        AgnoLinearTools.get_high_priority_issues,
    )
    def get_high_priority_issues(self) -> Optional[str]:
        return self._tool.get_high_priority_issues()
