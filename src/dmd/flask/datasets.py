from autoinject import injector, auto

import flask

from dmd.db import DataManagementDatabase
from dmd.metadata.datasets import Dataset
from gcapp import i18n
from gcapp.queries import SqlCondition, And, Equals, In, Or
from gcflask.datatables import DataQuery, DataTable, ObjectProperty
from gcflask.datatables.table import ActionListColumn
from gcflask.forms import GCFlaskForm, SubmitField
from gcflask.i18n_url import MultiLanguageBlueprint
from gcflask.security import require_permission
from gcflask.user import current_user

datasets = MultiLanguageBlueprint('datasets', __name__)


@datasets.route("/datasets")
@require_permission("datasets.view")
def list_datasets():
    links = []
    if current_user().require_all("dmd.datasets.create"):
        links.append((
            flask.url_for("datasets.create_datasets"),
            i18n.tr("dmd.datasets.create")
        ))
    table = _datasets_table()
    return flask.render_template(
        ["list_datasets.html", "data_table.html"],
        links=links,
        table=table,
    )


@datasets.route("/datasets/_ajax", methods=["POST", "GET"])
@require_permission("datasets.view", check_referrer=False)
def list_datasets_ajax():
    return _datasets_table().build_ajax()


@datasets.route("/datasets/new", methods=["POST", "GET"])
@require_permission("datasets.create")
def create_dataset():
    ...


@datasets.route("/datasets/<int:dataset_id>")
@require_permission("datasets.view")
def view_dataset(dataset_id: int):
    ...


@datasets.route("/datasets/<int:dataset_id>/<int:revision_no>", methods=["POST", "GET"])
def view_revision(dataset_id: int, revision_no: int):
    ...


@datasets.route("/datasets/<int:dataset_id>/<int:revision_no>/<profile_name>/<format_name>", methods=["POST", "GET"])
def view_formatted_metadata(dataset_id: int, revision_no: int, profile_name: str, format_name: str):
    return view_formatted_metadata_for_env(dataset_id, revision_no, profile_name, format_name, "live")


@datasets.route("/datasets/<int:dataset_id>/<int:revision_no>/<profile_name>/<format_name>/<environment>", methods=["POST", "GET"])
def view_formatted_metadata_for_env(dataset_id: int, revision_no: int, profile_name: str, format_name: str, environment: str):
    ...


@datasets.route("/datasets/<int:dataset_id>/edit", methods=["POST", "GET"])
@require_permission("datasets.edit")
def edit_dataset(dataset_id: int):
    ...


@datasets.route("/api/datasets/create", methods=["POST"])
@require_permission("datasets.create")
def create_dataset_api():
    ...


@datasets.route("/datasets/<int:dataset_id>/copy", methods=["POST", "GET"])
@require_permission("datasets.create")
def copy_dataset(dataset_id: int):
    ds = Dataset.find_by_id(dataset_id)
    if ds is None:
        return flask.abort(404)
    if not ds.can_access("copy"):
        return flask.abort(403)
    form = ConfirmCopyForm()
    if form.validate_on_submit():
        ds_id = ds.create_copy()
        return flask.redirect(flask.url_for("datasets.view_dataset", dataset_id=ds_id))
    return flask.render_template(
        ["copy_dataset.html", "dataset_form.html", "form.html"],
        dataset=ds,
        form=form
    )


class ConfirmCopyForm(GCFlaskForm):
    submit = SubmitField()


@datasets.route("/datasets/<int:dataset_id>/edit-metadata", methods=["POST", "GET"])
@require_permission("datasets.edit")
def edit_dataset_metadata(dataset_id: int):
    ...


@datasets.route("/datasets/<int:dataset_id>/edit-metadata/<display_group>", methods=["POST", "GET"])
@require_permission("datasets.edit")
def edit_dataset_metadata_for_group(dataset_id: int, display_group: str):
    ...


@datasets.route("/datasets/<int:dataset_id>/activate", methods=["POST", "GET"])
def activate_dataset(dataset_id: int):
    ds = Dataset.find_by_id(dataset_id)
    if ds is None:
        return flask.abort(404)
    if not ds.can_access("activate"):
        return flask.abort(403)
    form = ConfirmActivateForm()
    if form.validate_on_submit():
        result = ds.activate()
        # TODO: handle result
        return flask.redirect(ds.view_link())
    return flask.render_template(
        ["activate_dataset.html", "dataset_form.html", "form.html"],
        dataset=ds,
        form=form
    )


class ConfirmActivateForm(GCFlaskForm):
    submit = SubmitField()


@datasets.route("/datasets/<int:dataset_id>/attach", methods=["POST", "GET"])
def add_attachment(dataset_id: int):
    ...


@datasets.route("/datasets/<int:dataset_id>/publish", methods=["POST", "GET"])
def publish_dataset(dataset_id: int):
    ds = Dataset.find_by_id(dataset_id)
    if ds is None:
        return flask.abort(404)
    if not ds.can_access("publish"):
        return flask.abort(403)
    form = ConfirmPublishForm()
    if form.validate_on_submit():
        result = ds.publish(False)
        # TODO: handle result
        return flask.redirect(ds.view_link())
    return flask.render_template(
        ["publish_dataset.html", "dataset_form.html", "form.html"],
        dataset=ds,
        form=form
    )


class ConfirmPublishForm(GCFlaskForm):
    submit = SubmitField()


@datasets.route("/datasets/<int:dataset_id>/remove", methods=["POST", "GET"])
def remove_dataset(dataset_id: int):
    ds = Dataset.find_by_id(dataset_id)
    if ds is None:
        return flask.abort(404)
    if not ds.can_access("remove"):
        return flask.abort(403)
    form = ConfirmRemoveForm()
    if form.validate_on_submit():
        ds.remove()
        ds.save(False)
        return flask.redirect(ds.view_link())
    return flask.render_template(
        ["remove_dataset.html", "dataset_form.html", "form.html"],
        dataset=ds,
        form=form
    )


class ConfirmRemoveForm(GCFlaskForm):
    submit = SubmitField()


@datasets.route("/datasets/<int:dataset_id>/restore", methods=["POST", "GET"])
def restore_dataset(dataset_id: int):
    ds = Dataset.find_by_id(dataset_id)
    if ds is None:
        return flask.abort(404)
    if not ds.can_access("restore"):
        return flask.abort(403)
    form = ConfirmRestoreForm()
    if form.validate_on_submit():
        ds.restore()
        ds.save(False)
        return flask.redirect(ds.view_link())
    return flask.render_template(
        ["restore_dataset.html", "dataset_form.html", "form.html"],
        dataset=ds,
        form=form
    )


class ConfirmRestoreForm(GCFlaskForm):
    submit = SubmitField()


@injector.inject
def _datasets_table(database: DataManagementDatabase = auto()):
    table = DataTable(
        table_id="dataset_list",
        query=DataQuery(
            database.queryable_table("datasets"),
            filters=_dataset_filters(database, "view")
        ),
        ajax_route=flask.url_for("datasets.list_datasets_ajax"),
        default_order="dataset_id",
    )
    table.add_column(ObjectProperty(
        name="container_id",
        header_text=i18n.dtr("dmd.dataset.name"),
        allow_order=True
    ))
    table.add_column(ObjectProperty(
        name="display_names",
        header_text=i18n.dtr("dmd.dataset.display_name"),
        allow_search=True,
    ))
    table.add_column(ActionListColumn(
        action_cb=lambda x: x.actions()
    ))
    return table


def _dataset_filters(database: DataManagementDatabase,
                     op: str = "view") -> SqlCondition | None:
    conditions = []
    user = current_user()
    if not user.require_all("dmd.datasets.view_deprecated"):
        conditions.append(Equals("is_deprecated", False))
    if user.require_all(f"dmd.datasets.{op}.alll"):
        ...
    elif user.require_all((f"dmd.datasets.{op}.organization", "dmd.organization.manage_any")):
        ...
    else:
        ors = []
        with database as db:
            if user.require_all(f"dmd.datasets.{op}.organization"):
                ors.append(In("organization_id", db.user_organization_ids(user.get_id())))
            if user.require_all(f"dmd.datasets.{op}.assigned"):
                ors.append(In("dataset_id", db.user_dataset_ids(user.get_id())))
        if ors:
            if len(ors) == 1:
                conditions.append(ors[0])
            else:
                conditions.append(Or(*ors))
    if not conditions:
        return None
    else:
        return And(*conditions)
