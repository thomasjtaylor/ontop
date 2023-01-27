CREATE TABLE "植物" (
  "名" NVARCHAR(10),
  "使用部" NVARCHAR(10),
  "条件" NVARCHAR(10),
  PRIMARY KEY ("名", "使用部")
);
INSERT INTO "植物" ("名", "使用部", "条件") VALUES (N'しそ', N'葉', N'新鮮な');

CREATE TABLE "成分" (
  "皿"  NVARCHAR(10),
  "植物名" NVARCHAR(10),
  "使用部" NVARCHAR(10),
  FOREIGN KEY ("植物名", "使用部") REFERENCES "植物"("名", "使用部")
);
INSERT INTO "成分" ("皿", "植物名", "使用部") VALUES (N'しそのとまと', N'しそ', N'葉');