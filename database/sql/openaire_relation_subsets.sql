SELECT

*

FROM `academic-observatory.openaire.relation`

WHERE source.type = 'result' AND target.type = 'result'
---WHERE source.type = 'result' AND target.type = 'datasource'
---WHERE source.type = 'result' AND target.type = 'organization'
---WHERE source.type = 'result' AND target.type = 'project'
---WHERE source.type = 'organization' AND target.type = 'project'
---WHERE source.type = 'organization' AND target.type = 'datasource'
---WHERE source.type = `organization' AND target.type = 'organization'