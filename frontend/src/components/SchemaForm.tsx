import { useI18n } from '@/lib/i18n'
import type { JsonSchemaNode } from '@/lib/api'
import { FieldHint } from '@/components/FieldHint'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

// Loosely typed on purpose: this renders arbitrary JSON Schema shapes from
// 16 different connector Config structs, whose actual TS shape isn't known
// statically — the schema itself (not a TS type) drives what's valid here.
type JsonValue = unknown
type JsonObject = Record<string, unknown>

function resolveRef(schema: JsonSchemaNode, defs: Record<string, JsonSchemaNode>): JsonSchemaNode {
  if (!schema.$ref) return schema
  const name = schema.$ref.replace('#/$defs/', '')
  return defs[name] ?? schema
}

interface SchemaFormProps {
  schema: JsonSchemaNode
  defs: Record<string, JsonSchemaNode>
  value: JsonObject
  onChange: (value: JsonObject) => void
  idPrefix: string
}

/**
 * Renders real form fields from a connector's JSON Schema (see
 * nexus-server's `list_connectors_handler` / nexus-core's `submit_connector!`
 * macro) instead of a raw JSON textarea. Recurses into nested objects and
 * array-of-object fields (e.g. mongodb's `fields: MongoFieldSpec[]`) — only
 * handles the shapes `schemars` actually emits for our Config structs
 * (string/integer/number/boolean, string enum, array, object, `$ref` into
 * `$defs`), not the full JSON Schema spec.
 *
 * Each field's `description` (a Rust doc comment on the Config struct
 * field) shows up behind a clickable "ⓘ" next to the label (`FieldHint`)
 * instead of always-visible text — keeps a 10+ field form scannable.
 */
export function SchemaForm({ schema, defs, value, onChange, idPrefix }: SchemaFormProps) {
  const { t } = useI18n()
  const properties = schema.properties ?? {}
  const required = new Set(schema.required ?? [])

  const setField = (key: string, fieldValue: JsonValue) => {
    onChange({ ...value, [key]: fieldValue })
  }

  return (
    <div className="flex flex-col gap-3">
      {Object.entries(properties).map(([key, rawFieldSchema]) => {
        const fieldSchema = resolveRef(rawFieldSchema, defs)
        const fieldId = `${idPrefix}${key}`
        const label = `${key}${required.has(key) ? ' *' : ''}`

        if (fieldSchema.enum) {
          return (
            <div key={key}>
              <div className="flex items-center gap-1.5">
                <Label htmlFor={fieldId}>{label}</Label>
                {fieldSchema.description && <FieldHint text={fieldSchema.description} />}
              </div>
              <select
                id={fieldId}
                value={(value[key] as string) ?? ''}
                onChange={(e) => setField(key, e.target.value)}
                className="mt-1.5 flex h-9 w-full rounded-lg border border-input bg-transparent px-3 text-sm text-foreground outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="" disabled>
                  {t('schemaForm.select')}…
                </option>
                {fieldSchema.enum.map((option) => (
                  <option key={option} value={option}>
                    {option}
                  </option>
                ))}
              </select>
            </div>
          )
        }

        if (fieldSchema.type === 'boolean') {
          return (
            <div key={key} className="flex items-center gap-2">
              <input
                id={fieldId}
                type="checkbox"
                checked={Boolean(value[key] ?? fieldSchema.default ?? false)}
                onChange={(e) => setField(key, e.target.checked)}
                className="h-4 w-4 rounded border-input bg-transparent text-primary accent-primary outline-none focus:ring-2 focus:ring-ring"
              />
              <Label htmlFor={fieldId}>{label}</Label>
              {fieldSchema.description && <FieldHint text={fieldSchema.description} />}
            </div>
          )
        }

        if (fieldSchema.type === 'integer' || fieldSchema.type === 'number') {
          return (
            <div key={key}>
              <div className="flex items-center gap-1.5">
                <Label htmlFor={fieldId}>{label}</Label>
                {fieldSchema.description && <FieldHint text={fieldSchema.description} />}
              </div>
              <Input
                id={fieldId}
                type="number"
                value={value[key] === undefined || value[key] === null ? '' : String(value[key])}
                onChange={(e) => {
                  const raw = e.target.value
                  setField(key, raw === '' ? undefined : Number(raw))
                }}
                className="mt-1.5"
              />
            </div>
          )
        }

        if (fieldSchema.type === 'array') {
          return (
            <ArrayField
              key={key}
              label={label}
              description={fieldSchema.description}
              itemSchema={fieldSchema.items ? resolveRef(fieldSchema.items, defs) : {}}
              defs={defs}
              items={(value[key] as JsonValue[]) ?? []}
              onChange={(items) => setField(key, items)}
            />
          )
        }

        if (fieldSchema.type === 'object' && !fieldSchema.properties && fieldSchema.additionalProperties) {
          return (
            <MapField
              key={key}
              label={label}
              description={fieldSchema.description}
              value={(value[key] as Record<string, string>) ?? {}}
              onChange={(next) => setField(key, next)}
            />
          )
        }

        if (fieldSchema.type === 'object') {
          return (
            <fieldset key={key} className="rounded-lg border border-white/10 p-3">
              <legend className="flex items-center gap-1.5 px-1 text-xs text-muted-foreground">
                {label}
                {fieldSchema.description && <FieldHint text={fieldSchema.description} />}
              </legend>
              <SchemaForm
                schema={fieldSchema}
                defs={defs}
                value={(value[key] as JsonObject) ?? {}}
                onChange={(nested) => setField(key, nested)}
                idPrefix={`${key}-`}
              />
            </fieldset>
          )
        }

        // Default: plain string field.
        return (
          <div key={key}>
            <div className="flex items-center gap-1.5">
              <Label htmlFor={fieldId}>{label}</Label>
              {fieldSchema.description && <FieldHint text={fieldSchema.description} />}
            </div>
            <Input
              id={fieldId}
              value={(value[key] as string) ?? ''}
              onChange={(e) => setField(key, e.target.value)}
              className="mt-1.5"
            />
          </div>
        )
      })}
    </div>
  )
}

interface MapFieldProps {
  label: string
  description?: string
  value: Record<string, string>
  onChange: (value: Record<string, string>) => void
}

/**
 * Renders a free-form `HashMap<String, String>` Rust field (e.g. REST's
 * `headers`, CSV's `storage_options`) as editable key/value rows — schemars
 * emits `{type: "object", additionalProperties: {...}}` for these with no
 * fixed `properties`, so the regular object branch above (which recurses
 * into a known property set) has nothing to render for them.
 */
function MapField({ label, description, value, onChange }: MapFieldProps) {
  const { t } = useI18n()
  const entries = Object.entries(value)

  const updateKey = (index: number, newKey: string) => {
    const next = entries.map(([k, v], i) => (i === index ? [newKey, v] : [k, v]))
    onChange(Object.fromEntries(next))
  }

  const updateValue = (index: number, newValue: string) => {
    const next = entries.map(([k, v], i) => (i === index ? [k, newValue] : [k, v]))
    onChange(Object.fromEntries(next))
  }

  const removeEntry = (index: number) => {
    onChange(Object.fromEntries(entries.filter((_, i) => i !== index)))
  }

  const addEntry = () => {
    onChange(Object.fromEntries([...entries, ['', '']]))
  }

  return (
    <fieldset className="rounded-lg border border-white/10 p-3">
      <legend className="flex items-center gap-1.5 px-1 text-xs text-muted-foreground">
        {label}
        {description && <FieldHint text={description} />}
      </legend>
      <div className="flex flex-col gap-2">
        {entries.map(([entryKey, entryValue], index) => (
          <div key={index} className="flex items-center gap-2">
            <Input
              placeholder={t('schemaForm.mapKey')}
              value={entryKey}
              onChange={(e) => updateKey(index, e.target.value)}
              className="flex-1"
            />
            <Input
              placeholder={t('schemaForm.mapValue')}
              value={entryValue}
              onChange={(e) => updateValue(index, e.target.value)}
              className="flex-1"
            />
            <button
              type="button"
              onClick={() => removeEntry(index)}
              className="text-xs text-red-400 hover:underline"
            >
              {t('common.remove')}
            </button>
          </div>
        ))}
      </div>
      <button type="button" onClick={addEntry} className="mt-2 text-xs text-primary hover:underline">
        {t('common.add')}
      </button>
    </fieldset>
  )
}

interface ArrayFieldProps {
  label: string
  description?: string
  itemSchema: JsonSchemaNode
  defs: Record<string, JsonSchemaNode>
  items: JsonValue[]
  onChange: (items: JsonValue[]) => void
}

function ArrayField({ label, description, itemSchema, defs, items, onChange }: ArrayFieldProps) {
  const { t } = useI18n()
  const isObjectItem = itemSchema.type === 'object'

  const updateItem = (index: number, next: JsonValue) => {
    const copy = [...items]
    copy[index] = next
    onChange(copy)
  }

  const removeItem = (index: number) => {
    onChange(items.filter((_, i) => i !== index))
  }

  const addItem = () => {
    onChange([...items, isObjectItem ? {} : ''])
  }

  return (
    <fieldset className="rounded-lg border border-white/10 p-3">
      <legend className="flex items-center gap-1.5 px-1 text-xs text-muted-foreground">
        {label}
        {description && <FieldHint text={description} />}
      </legend>
      <div className="flex flex-col gap-2">
        {items.map((item, index) => (
          <div key={index} className="flex items-start gap-2 rounded-md border border-white/10 p-2">
            <div className="flex-1">
              {isObjectItem ? (
                <SchemaForm
                  schema={itemSchema}
                  defs={defs}
                  value={(item as JsonObject) ?? {}}
                  onChange={(next) => updateItem(index, next)}
                  idPrefix={`item-${index}-`}
                />
              ) : (
                <Input
                  value={(item as string) ?? ''}
                  onChange={(e) => updateItem(index, e.target.value)}
                />
              )}
            </div>
            <button
              type="button"
              onClick={() => removeItem(index)}
              className="text-xs text-red-400 hover:underline"
            >
              {t('common.remove')}
            </button>
          </div>
        ))}
      </div>
      <button
        type="button"
        onClick={addItem}
        className="mt-2 text-xs text-primary hover:underline"
      >
        {t('common.add')}
      </button>
    </fieldset>
  )
}
