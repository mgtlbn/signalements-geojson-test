const https = require('https');
const fs = require('fs');

// ============================================
// CONFIGURATION
// ============================================
const GRIST_DOC_ID = process.env.GRIST_DOC_ID;
const GRIST_API_KEY = process.env.GRIST_API_KEY;
const TABLE_SIGNALEMENTS = 'Signalements';
const TABLE_CD44 = 'Routes_CD44'; // Nom de votre table pour les données du 44
const TABLE_RENNES = 'Routes_Rennes'; // Nom de votre table pour les données Rennes

if (!GRIST_DOC_ID || !GRIST_API_KEY) {
    console.error('❌ Variables d\'environnement manquantes');
    process.exit(1);
}

// ============================================
// FONCTIONS UTILITAIRES
// ============================================

/**
 * Récupère les données d'une table Grist
 */
function fetchGristTable(tableName) {
    return new Promise((resolve, reject) => {
        const options = {
            hostname: 'grist.dataregion.fr',
            path: `/o/inforoute/api/docs/${GRIST_DOC_ID}/tables/${tableName}/records`,
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${GRIST_API_KEY}`,
                'Content-Type': 'application/json'
            }
        };

        console.log(`🔗 Récupération table: ${tableName}`);

        https.get(options, (res) => {
            let data = '';
            
            res.on('data', (chunk) => {
                data += chunk;
            });
            
            res.on('end', () => {
                if (res.statusCode !== 200) {
                    console.error(`❌ Erreur ${res.statusCode} pour ${tableName}`);
                    reject(new Error(`Erreur API: ${res.statusCode}`));
                } else {
                    try {
                        const parsed = JSON.parse(data);
                        console.log(`✅ ${tableName}: ${parsed.records ? parsed.records.length : 0} enregistrements`);
                        resolve(parsed);
                    } catch (e) {
                        reject(new Error('Erreur parsing JSON: ' + e.message));
                    }
                }
            });
        }).on('error', (err) => {
            reject(err);
        });
    });
}

/**
 * Convertit un enregistrement Grist en feature GeoJSON
 */
function recordToFeature(record, source) {
    try {
        let geometry;
        const fields = record.fields;
        
        // Gérer le format GeoJSON dans le champ geojson
        if (fields.geojson) {
            geometry = JSON.parse(fields.geojson);
        }
        // Gérer le format Latitude/Longitude
        else if (fields.Latitude && fields.Longitude) {
            if (fields.Latitude_fin && fields.Longitude_fin) {
                geometry = {
                    type: 'LineString',
                    coordinates: [
                        [fields.Longitude, fields.Latitude],
                        [fields.Longitude_fin, fields.Latitude_fin]
                    ]
                };
            } else {
                geometry = {
                    type: 'Point',
                    coordinates: [fields.Longitude, fields.Latitude]
                };
            }
        } else {
            return null; // Pas de géométrie valide
        }
        
        // Construire les propriétés en normalisant les champs
        return {
            type: 'Feature',
            geometry: geometry,
            properties: {
                id: `${source}_${record.id}`,
                source: source,
                geometry_type: fields.geometrie_type || geometry.type,
                
                // Champs principaux (normalisés)
                route: fields.Route || fields.route || '',
                commune: fields.Commune || fields.commune || '',
                type_coupure: fields.Type_coupure || fields.type_coupure || '',
                sens_circulation: fields.Sens_circulation || fields.sens_circulation || 'N/A',
                priorite: fields.Priorite || fields.priorite || 'Moyenne',
                statut: fields.Statut || fields.statut || 'Actif',
                
                // Cause(s)
                cause: Array.isArray(fields.Cause) ? 
                       fields.Cause.join(', ') : 
                       (fields.Cause || fields.cause || ''),
                
                // Administration/Gestionnaire
                administration: fields.Administration || fields.administration || '',
                gestionnaire: fields.Gestionnaire || fields.gestionnaire || null,
                agence: fields.Agence || fields.agence || null,
                
                // Informations complémentaires
                description: fields.Description || fields.description || '',
                restrictions: fields.Restrictions || fields.restrictions || '',
                lineaire_inonde: fields.Lineaire_inonde || fields.lineaire_inonde || '',
                evolution: fields.Evolution || fields.evolution || '',
                
                // Champs spécifiques CD35
                prd: fields.PRD || fields.prd || null,
                prf: fields.PRF || fields.prf || null,
                agent: fields.Agent || fields.agent || null,
                contact: fields.Contact || fields.contact || null,
                utilisateur: fields.Utilisateur || fields.utilisateur || null,
                
                // Dates
                date_heure: fields.Date_heure || fields.date_heure || fields.Date_debut || fields.date_debut || '',
                date_debut: fields.Date_debut || fields.date_debut || null,
                date_fin: fields.Date_fin || fields.date_fin || null
            }
        };
    } catch (e) {
        console.warn(`⚠️ Erreur pour l'enregistrement ${record.id} (${source}):`, e.message);
        return null;
    }
}

/**
 * Filtre les features pour ne garder que celles avec des valeurs non-null
 */
function cleanProperties(feature) {
    const cleaned = { ...feature };
    const props = cleaned.properties;
    
    // Supprimer les propriétés null, undefined ou chaînes vides
    Object.keys(props).forEach(key => {
        if (props[key] === null || props[key] === undefined || props[key] === '') {
            delete props[key];
        }
    });
    
    return cleaned;
}

// ============================================
// FONCTION PRINCIPALE
// ============================================

async function generateUnifiedGeoJSON() {
    try {
        console.log('🚀 Démarrage de la fusion des données...\n');
        
        const allFeatures = [];
        
        // =============================================
        // 1️⃣ RÉCUPÉRER LES SIGNALEMENTS GRIST (CD35)
        // =============================================
        try {
            console.log('📋 Étape 1/3: Signalements Ille-et-Vilaine (CD35)');
            const gristData = await fetchGristTable(TABLE_SIGNALEMENTS);
            
            const gristFeatures = gristData.records
                .filter(record => {
                    return record.fields.geojson || 
                           (record.fields.Latitude && record.fields.Longitude);
                })
                .map(record => recordToFeature(record, 'CD35'))
                .filter(f => f !== null)
                .map(cleanProperties);
            
            allFeatures.push(...gristFeatures);
            console.log(`✅ ${gristFeatures.length} signalements CD35 ajoutés\n`);
        } catch (error) {
            console.error('⚠️ Erreur lors de la récupération des signalements CD35:', error.message);
        }
        
        // =============================================
        // 2️⃣ RÉCUPÉRER LES DONNÉES CD44
        // =============================================
        try {
            console.log('📋 Étape 2/3: Données Loire-Atlantique (CD44)');
            const cd44Data = await fetchGristTable(TABLE_CD44);
            
            const cd44Features = cd44Data.records
                .filter(record => {
                    return record.fields.geojson || 
                           (record.fields.Latitude && record.fields.Longitude);
                })
                .map(record => recordToFeature(record, 'CD44'))
                .filter(f => f !== null)
                .map(cleanProperties);
            
            allFeatures.push(...cd44Features);
            console.log(`✅ ${cd44Features.length} signalements CD44 ajoutés\n`);
        } catch (error) {
            console.error('⚠️ Erreur lors de la récupération des données CD44:', error.message);
            console.error('   → Assurez-vous que la table "Routes_CD44" existe dans Grist');
        }
        
        // =============================================
        // 3️⃣ RÉCUPÉRER LES DONNÉES RENNES MÉTROPOLE
        // =============================================
        try {
            console.log('📋 Étape 3/3: Données Rennes Métropole');
            const rennesData = await fetchGristTable(TABLE_RENNES);
            
            const rennesFeatures = rennesData.records
                .filter(record => {
                    return record.fields.geojson || 
                           (record.fields.Latitude && record.fields.Longitude);
                })
                .map(record => recordToFeature(record, 'Rennes Metropole'))
                .filter(f => f !== null)
                .map(cleanProperties);
            
            allFeatures.push(...rennesFeatures);
            console.log(`✅ ${rennesFeatures.length} signalements Rennes Métropole ajoutés\n`);
        } catch (error) {
            console.error('⚠️ Erreur lors de la récupération des données Rennes:', error.message);
            console.error('   → Assurez-vous que la table "Routes_Rennes" existe dans Grist');
        }
        
        // =============================================
        // 4️⃣ CRÉER LE GEOJSON UNIFIÉ
        // =============================================
        console.log('🔨 Création du GeoJSON unifié...');
        
        const geojson = {
            type: 'FeatureCollection',
            features: allFeatures,
            metadata: {
                generated: new Date().toISOString(),
                sources: {
                    cd35: allFeatures.filter(f => f.properties.source === 'CD35').length,
                    cd44: allFeatures.filter(f => f.properties.source === 'CD44').length,
                    rennes: allFeatures.filter(f => f.properties.source === 'Rennes Metropole').length
                },
                total_features: allFeatures.length,
                doc_id: GRIST_DOC_ID
            }
        };
        
        // =============================================
        // 5️⃣ ÉCRIRE LES FICHIERS
        // =============================================
        const filename = 'signalements_routes_fusionnes.geojson';
        fs.writeFileSync(filename, JSON.stringify(geojson, null, 2));
        console.log(`✅ Fichier ${filename} créé avec succès !`);
        
        // Créer aussi le fichier de métadonnées
        const metadata = {
            date_generation: new Date().toISOString(),
            sources: geojson.metadata.sources,
            total: geojson.metadata.total_features
        };
        fs.writeFileSync('metadata.json', JSON.stringify(metadata, null, 2));
        console.log('✅ Fichier metadata.json créé avec succès !');
        
        // =============================================
        // 6️⃣ AFFICHER LES STATISTIQUES
        // =============================================
        console.log('\n📊 STATISTIQUES FINALES:');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`📍 CD35 (Ille-et-Vilaine):    ${geojson.metadata.sources.cd35} signalements`);
        console.log(`📍 CD44 (Loire-Atlantique):   ${geojson.metadata.sources.cd44} signalements`);
        console.log(`📍 Rennes Métropole:          ${geojson.metadata.sources.rennes} signalements`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`🎯 TOTAL:                     ${geojson.metadata.total_features} signalements`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
        
    } catch (error) {
        console.error('💥 Erreur fatale:', error);
        process.exit(1);
    }
}

// Lancer la génération
generateUnifiedGeoJSON();
