Descriptions of the lineage columns that ingestion adds to every raw table and models
carry forward, referenced with `description: "{{ doc('<column>') }}"`.

{% docs _source_file %}
URI of the file the row was loaded from.
{% enddocs %}

{% docs _source_row %}
Position of the row in the source file, starting at 1.
{% enddocs %}

{% docs _loaded_at %}
When the row was loaded; shared by all rows of one load.
{% enddocs %}

{% docs _load_id %}
The load's entry in audit.load_log (run, attempt, row counts).
{% enddocs %}
