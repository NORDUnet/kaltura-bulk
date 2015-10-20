import csv
import os
import xml.etree.cElementTree as ET

FIELDS = ["mediaType", "name", "description", "downloadUrl", "userId", "tags", "categories", "startDate", "endDate" ]

def create_item(name, description, downloadUrl, userId, tags, categories, startDate, endDate, mediaType):
    item = ET.Element("item")
    ET.SubElement(item, "action").text="add"
    ET.SubElement(item, "type").text="1"
    ET.SubElement(item, "userId").text=userId
    ET.SubElement(item, "name").text=name
    dl = ET.SubElement(item, "contentAssets")
    dl2 = ET.SubElement(dl, "content")
    ET.SubElement(dl2, "urlContentResource", url=downloadUrl)
    if tags:
        tagsElm = ET.SubElement(item, "tags")
        for t in tags:
            ET.SubElement(tagsElm, "tag").text=t
    if categories:
        categoriesElm = ET.SubElement(item, "categories")
        for c in categories:
            ET.SubElement(categoriesElm, "category").text=c
    if startDate:
        ET.SubElement(item, "startDate").text=startDate
    if endDate:
        ET.SubElement(item, "endDate").text=endDate
    media = ET.SubElement(item, "media")
    ET.SubElement(media, "mediaType").text=mediaType
    return item

def parse_fields(headers):
    return dict([ (field, headers.index(field))  for field in FIELDS ])

def write_bulk_file(items, name="bulk_upload", nbr=1, out_dir=None):
    path = name
    if out_dir:
        path = os.path.join(out_dir, path)
    root = ET.Element("mrss", {"xmlns:xsd": "http://www.w3.org/2001/XMLSchema", "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance", "xsi:noNamespaceSchemaLocation": "ingestion.xsd"})
    channel = ET.SubElement(root, "channel")
    channel.extend(items)
    try:
      out = ET.tostring(root, encoding="utf-8")
      with open("{0}_{1:03d}.xml".format(path, nbr), "wb") as f:
          f.write(out)
    except UnicodeDecodeError as e: 
      print e

def bad_row(row, out_dir=None):
    path = "bad_rows.txt"
    if out_dir:
        path = os.path.join(out_dir, path)
    with open(path, "a") as bad:
        bad.write(";".join(row)+"\n")

def is_bad(row):
    bad = False
    try:
        ";".join(row).decode("utf-8")
    except UnicodeDecodeError:
        bad = True
    return bad

def process(f, base_name, split_size=250, out_dir=None):
    with open(f, "rU") as csvfile:
        lines = csv.reader(csvfile, delimiter=";")
        fields = parse_fields(lines.next())
        items = []
        file_nbr=1
        for row in lines:
            if is_bad(row):
                bad_row(row, out_dir=out_dir)
                continue
            name = row[fields["name"]]
            description = row[fields["description"]]
            downloadUrl = row[fields["downloadUrl"]]
            userId = row[fields["userId"]]
            tags = row[fields["tags"]]
            if tags:
                tags = [t.strip() for t in tags.split(",")]
            categories = row[fields["categories"]]
            if categories:
                categories = [c.strip() for c in categories.split(",")]
            startDate = row[fields["startDate"]]
            endDate = row[fields["endDate"]]
            mediaType = row[fields["mediaType"]]
            items.append(create_item(name, description, downloadUrl, userId, tags, categories, startDate, endDate, mediaType))
            if len(items) >= split_size:
                write_bulk_file(items,name=base_name, nbr=file_nbr, out_dir=out_dir)
                file_nbr +=1
                items = []
        # write last file
        if items:
            write_bulk_file(items, name=base_name, nbr=file_nbr, out_dir=out_dir)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Kaltura bulk converter ")
    parser.add_argument("csvfile", help="the csv file to process")
    parser.add_argument("-n","--base-name", help="the output base name default='bulk_upload'", default="bulk_upload")
    parser.add_argument("-d", "--outdir", help="the directory to put all the files in", required=False, default=None)
    parser.add_argument("-s", "--split-size", type=int, help="how many items should be in a single bulk file", default=200)
    args = parser.parse_args()
    if args.outdir and not os.path.exists(args.outdir):
        os.makedirs(args.outdir)
    process(args.csvfile, args.base_name, args.split_size, out_dir=args.outdir)
